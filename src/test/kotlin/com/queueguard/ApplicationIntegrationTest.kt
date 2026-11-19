package com.queueguard

import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.boot.test.system.CapturedOutput
import org.springframework.boot.test.system.OutputCaptureExtension
import org.springframework.context.annotation.Import
import com.queueguard.api.ApiException
import com.queueguard.api.ErrorCode
import com.queueguard.config.QueuePolicy
import com.queueguard.redis.PolicyStore
import com.queueguard.redis.QueueKeys
import jakarta.validation.Valid
import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.Size
import org.springframework.data.redis.RedisConnectionFailureException
import org.springframework.web.bind.annotation.*
import java.util.concurrent.Executors
import java.util.concurrent.Callable
import java.util.concurrent.TimeUnit
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
@ExtendWith(OutputCaptureExtension::class)
@Import(ApplicationIntegrationTest.ContractProbeController::class)
@ActiveProfiles("test")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ApplicationIntegrationTest {
    @LocalServerPort
    private var port: Int = 0

    @Autowired
    private lateinit var redis: StringRedisTemplate

    @Autowired
    private lateinit var policies: PolicyStore

    private val client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()

    @Test
    fun `health is public without connection details`() {
        val response = get("/actuator/health")
        assertEquals(200, response.statusCode())
        assertTrue(response.body().contains("\"status\":\"UP\""))
        assertFalse(response.body().contains("components"))
    }

    @Test
    fun `business paths and other actuator endpoints are closed`() {
        for (path in listOf("/v1/queues", "/actuator/env", "/actuator/health/redis")) {
            assertEquals(401, get(path).statusCode(), path)
        }
    }

    @Test
    fun `real Redis supports expiring writes`() {
        val key = "queueguard:s1:test:${UUID.randomUUID()}"
        try {
            redis.opsForValue().set(key, "connected", Duration.ofSeconds(30))
            assertEquals("connected", redis.opsForValue().get(key))
            assertTrue(redis.getExpire(key) in 1..30)
        } finally {
            redis.delete(key)
        }
    }

    @Test
    fun `service identity comes from token and does not survive the request`() {
        val response = request("/v1/queues/commerce-orders/policy", TOKEN, spoofClient = "events")
        assertEquals(200, response.statusCode())
        assertTrue(response.body().contains("\"clientId\":\"commerce\""))
        assertFalse(response.body().contains(TOKEN))
        assertEquals("no-store", response.headers().firstValue("Cache-Control").orElseThrow())
        assertFalse(response.headers().firstValue("Set-Cookie").isPresent)
        assertEquals(401, get("/v1/queues/commerce-orders/policy").statusCode())
    }

    @Test
    fun `invalid authentication is sanitized and challenged`(output: CapturedOutput) {
        for (token in listOf("bad-secret", "wrong-service-token-never-accepted-00001")) {
            val response = request("/v1/queues/commerce-orders/policy", token)
            assertError(response, 401, "UNAUTHORIZED")
            assertEquals("Bearer", response.headers().firstValue("WWW-Authenticate").orElseThrow())
            assertFalse(response.body().contains(token))
            assertFalse(output.all.contains(token))
        }
    }

    @Test
    fun `queue access is scoped and invalid identifiers cannot affect namespaces`() {
        assertError(request("/v1/queues/event-signup/policy", TOKEN), 403, "FORBIDDEN")
        assertError(request("/v1/queues/missing/policy", TOKEN), 404, "NOT_FOUND")
        assertError(request("/v1/queues/bad%7Btag%7D/policy", TOKEN), 400, "INVALID_REQUEST")
        assertError(request("/actuator/env", TOKEN), 403, "FORBIDDEN")
        assertError(request("/v1/queues/commerce-orders/missing", TOKEN), 404, "NOT_FOUND")
    }

    @Test
    fun `validation malformed JSON and failure responses do not echo input`(output: CapturedOutput) {
        for (body in listOf("{", "{\"subjectId\":\"\",\"secret\":\"$TOKEN\"}")) {
            val response = request("/v1/queues/commerce-orders/probe", TOKEN, body)
            assertError(response, 400, "INVALID_REQUEST")
            assertFalse(response.body().contains(TOKEN))
        }
        assertEquals(200, request("/v1/queues/commerce-orders/probe", TOKEN, "{\"subjectId\":\"user-1\"}").statusCode())
        for ((code, status) in listOf(ErrorCode.CONFLICT to 409, ErrorCode.EXPIRED to 410, ErrorCode.CAPACITY_EXCEEDED to 429)) {
            assertError(request("/v1/queues/commerce-orders/failure/${code.name}", TOKEN), status, code.name)
        }
        assertError(request("/v1/queues/commerce-orders/failure/redis", TOKEN), 503, "UNAVAILABLE")
        assertError(request("/v1/queues/commerce-orders/failure/internal", TOKEN), 500, "INTERNAL_ERROR")
        assertFalse(output.all.contains(TOKEN))
    }

    @Test
    fun `unsupported and malformed Accept return JSON client errors`(output: CapturedOutput) {
        for ((accept, status, code) in listOf(
            Triple("text/plain", 406, "NOT_ACCEPTABLE"),
            Triple("not/a/media/type", 400, "INVALID_REQUEST"),
        )) {
            val response = request("/v1/queues/commerce-orders/policy", TOKEN, accept = accept)
            assertError(response, status, code)
            assertTrue(response.headers().firstValue("Content-Type").orElseThrow().startsWith("application/json"))
            assertFalse(response.headers().firstValue("WWW-Authenticate").isPresent)
        }
        assertError(request("/v1/queues/commerce-orders/probe", TOKEN, "{", accept = "text/plain"), 400, "INVALID_REQUEST")
        for ((code, status) in listOf("CONFLICT" to 409, "redis" to 503, "internal" to 500)) {
            val expected = when (code) {
                "redis" -> "UNAVAILABLE"
                "internal" -> "INTERNAL_ERROR"
                else -> code
            }
            assertError(request("/v1/queues/commerce-orders/failure/$code", TOKEN, accept = "text/plain"), status, expected)
        }
        assertFalse(output.all.contains("Failure in @ExceptionHandler"))
        assertFalse(output.all.contains(TOKEN))
    }

    @Test
    fun `encoded routes cannot bypass the common queue permission check`() {
        for (prefix in listOf("/v1/queues", "/v1/%71ueues")) {
            assertError(request("$prefix/event-signup/probe", TOKEN, "{\"subjectId\":\"user-1\"}"), 403, "FORBIDDEN")
            assertEquals(200, request("$prefix/commerce-orders/probe", TOKEN, "{\"subjectId\":\"user-1\"}").statusCode())
        }
        assertEquals(200, request("/v1/queues/%63ommerce-orders/probe", TOKEN, "{\"subjectId\":\"user-1\"}").statusCode())
        assertError(request("/v1/queues/%65vent-signup/probe", TOKEN, "{\"subjectId\":\"user-1\"}"), 403, "FORBIDDEN")
        assertEquals(200, get("/actuator/%68ealth").statusCode())
    }

    @Test
    fun `logout login and error paths stay closed without redirects`() {
        for (path in listOf("/logout", "/login", "/error")) {
            for (method in listOf("GET", "POST")) {
                val anonymous = request(path, method = method)
                assertError(anonymous, 401, "UNAUTHORIZED")
                assertFalse(anonymous.headers().firstValue("Location").isPresent)
                val authenticated = request(path, TOKEN, method = method)
                assertError(authenticated, 403, "FORBIDDEN")
                assertFalse(authenticated.headers().firstValue("Location").isPresent)
            }
        }
    }

    @Test
    fun `unsupported methods preserve the Allow response header`() {
        val response = request("/v1/queues/commerce-orders/policy", TOKEN, method = "POST")
        assertError(response, 405, "METHOD_NOT_ALLOWED")
        val allowed = response.headers().firstValue("Allow").orElseThrow().split(',').map(String::trim)
        assertTrue("GET" in allowed)
        assertFalse("POST" in allowed)
    }

    @Test
    fun `policy initialization is atomic idempotent and never overwrites different settings`() {
        val policy = QueuePolicy("concurrent-${UUID.randomUUID()}")
        val key = QueueKeys("commerce", policy.queueId).config
        val pool = Executors.newFixedThreadPool(8)
        try {
            val tasks = List(24) { Callable { policies.initialize("commerce", policy) } }
            pool.invokeAll(tasks).forEach { it.get(10, TimeUnit.SECONDS) }
            val before = redis.opsForHash<String, String>().entries(key)
            kotlin.test.assertFailsWith<IllegalStateException> {
                policies.initialize("commerce", policy.copy(maxActiveAdmissions = 2))
            }
            assertEquals(before, redis.opsForHash<String, String>().entries(key))
        } finally {
            pool.shutdownNow()
            redis.delete(key)
        }
    }

    @Test
    fun `missing policy with existing state and wrong key type refuse initialization`() {
        for (suffix in listOf(":config", ":sequence")) {
            val policy = QueuePolicy("corrupt-${UUID.randomUUID()}")
            val keys = QueueKeys("commerce", policy.queueId)
            val key = keys.config.removeSuffix(":config") + suffix
            try {
                redis.opsForValue().set(key, "existing-state")
                kotlin.test.assertFailsWith<IllegalStateException> { policies.initialize("commerce", policy) }
                assertEquals("existing-state", redis.opsForValue().get(key))
                if (suffix != ":config") assertFalse(redis.hasKey(keys.config))
            } finally {
                redis.delete(keys.all)
            }
        }
    }

    private fun assertError(response: HttpResponse<String>, status: Int, code: String) {
        assertEquals(status, response.statusCode(), response.body())
        assertTrue(response.body().contains("\"errorCode\":\"$code\""), response.body())
        assertEquals("no-store", response.headers().firstValue("Cache-Control").orElseThrow())
    }

    private fun get(path: String): HttpResponse<String> = request(path)

    private fun request(
        path: String, token: String? = null, body: String? = null, spoofClient: String? = null,
        accept: String? = null, method: String = if (body == null) "GET" else "POST",
    ): HttpResponse<String> {
        val builder = HttpRequest.newBuilder(URI("http://127.0.0.1:$port$path")).timeout(Duration.ofSeconds(10))
        if (token != null) builder.header("Authorization", "Bearer $token")
        if (spoofClient != null) builder.header("X-Client-Id", spoofClient)
        if (accept != null) builder.header("Accept", accept)
        if (body != null) builder.header("Content-Type", "application/json")
        builder.method(method, body?.let(HttpRequest.BodyPublishers::ofString) ?: HttpRequest.BodyPublishers.noBody())
        return client.send(builder.build(), HttpResponse.BodyHandlers.ofString())
    }

    data class ProbeRequest(@field:NotBlank @field:Size(max = 128) val subjectId: String)

    @RestController
    class ContractProbeController {
        @PostMapping("/v1/queues/{queueId}/probe")
        fun validate(@Valid @RequestBody request: ProbeRequest) = mapOf("accepted" to true)

        @GetMapping("/v1/queues/{queueId}/failure/{code}")
        fun fail(@PathVariable code: String): String = when (code) {
            "redis" -> throw RedisConnectionFailureException(TOKEN)
            "internal" -> throw IllegalStateException(TOKEN)
            else -> throw ApiException(ErrorCode.valueOf(code))
        }
    }

    companion object {
        private const val TOKEN = "test-commerce-token-not-for-production-0001"
        @Container
        @JvmStatic
        val redisContainer = GenericContainer<Nothing>("redis:7.4-alpine")
            .apply { withExposedPorts(6379) }

        @DynamicPropertySource
        @JvmStatic
        fun redisProperties(registry: DynamicPropertyRegistry) {
            registry.add("spring.data.redis.host", redisContainer::getHost)
            registry.add("spring.data.redis.port") { redisContainer.getMappedPort(6379) }
        }
    }
}
