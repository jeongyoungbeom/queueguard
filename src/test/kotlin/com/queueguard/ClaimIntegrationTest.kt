package com.queueguard

import com.queueguard.api.*
import com.queueguard.redis.*
import com.queueguard.service.*
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.Base64
import java.util.UUID
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.system.CapturedOutput
import org.springframework.boot.test.system.OutputCaptureExtension
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.dao.DataAccessException
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import tools.jackson.databind.ObjectMapper
import tools.jackson.databind.node.ObjectNode

@Testcontainers
@ExtendWith(OutputCaptureExtension::class)
@ActiveProfiles("admissions-test")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ClaimIntegrationTest {
    @LocalServerPort private var port: Int = 0
    @Autowired private lateinit var service: EntryService
    @Autowired private lateinit var store: EntryStore
    @Autowired private lateinit var registry: QueueRegistry
    @Autowired private lateinit var policies: PolicyStore
    @Autowired private lateinit var redis: StringRedisTemplate
    @Autowired private lateinit var mapper: ObjectMapper
    private val client = HttpClient.newHttpClient()
    private val fingerprint = "a".repeat(64)

    @BeforeEach
    fun reset() {
        registry.allPolicies().forEach { (clientId, policy) ->
            redis.delete(QueueKeys(clientId, policy.queueId).all)
            policies.initialize(clientId, policy)
        }
    }

    @Test
    fun `client following the polling hint can claim and release a one second admission`() {
        val waiting = service.register("commerce", "fast-expiry", "owner").entry
        val delay = assertNotNull(waiting.pollAfterMillis)
        val ttlMillis = registry.policy("commerce", "fast-expiry").admissionTtlSeconds * 1000L
        assertTrue(delay > 0 && delay < ttlMillis, "Polling hint $delay must be shorter than TTL $ttlMillis")
        assertEquals(delay, service.current("commerce", "fast-expiry", "owner").pollAfterMillis)
        admit("fast-expiry")
        Thread.sleep(delay)
        val ticket = service.current("commerce", "fast-expiry", "owner")
        assertEquals(EntryState.ADMITTED, ticket.state)
        val claimed = post(ticket, "claim")
        assertEquals(200, claimed.statusCode(), claimed.body())
        assertEquals("CLAIMED", body(claimed)["state"].asString())
        assertEquals(ticket.expiresAt.toString(), body(claimed)["expiresAt"].asString())
        val released = post(ticket, "release")
        assertEquals(200, released.statusCode(), released.body())
        assertEquals("RELEASED", body(released)["state"].asString())
        assertNull(service.current("commerce", "fast-expiry", "owner").pollAfterMillis)
        assertEquals(0, service.statistics("commerce", "fast-expiry").activeAdmissions)
    }

    @Test
    fun `claim retries and registration preserve the binding slot token and original deadline`(output: CapturedOutput) {
        val ticket = ticket()
        val response = post(ticket, "claim")
        assertEquals(200, response.statusCode(), response.body())
        assertEquals("CLAIMED", body(response)["state"].asString())
        assertEquals("request-1", body(response)["requestId"].asString())
        assertEquals(ticket.expiresAt.toString(), body(response)["expiresAt"].asString())
        assertFalse(response.body().contains(ticket.admissionToken!!))
        assertFalse(response.body().contains("admissionToken"))
        val before = snapshot()
        assertEquals(body(response), body(post(ticket, "claim"))) // 첫 응답을 잃었을 때와 같은 재시도다.
        assertEquals(before, snapshot())
        val current = service.current("commerce", "fifo", "owner")
        assertEquals(EntryState.CLAIMED, current.state)
        assertEquals("request-1", current.requestId)
        assertEquals(ticket.admissionToken, current.admissionToken)
        assertEquals(ticket.expiresAt, current.expiresAt)
        val registered = service.register("commerce", "fifo", "owner")
        assertFalse(registered.created)
        assertEquals(current, registered.entry)
        assertEquals(1, service.statistics("commerce", "fifo").activeAdmissions)
        assertEquals(fingerprint, admission(ticket)["requestFingerprint"].asString())
        assertFalse(output.all.contains(ticket.admissionToken))
        assertFalse(ClaimRequest("owner", ticket.admissionToken, "request-1", fingerprint).toString().contains(ticket.admissionToken))
    }

    @Test
    fun `different parallel requests bind one ticket exactly once`() {
        val ticket = ticket()
        val results = parallel(48) { post(ticket, "claim", requestId = "request-$it") }
        assertEquals(1, results.count { it.statusCode() == 200 })
        assertEquals(47, results.count { it.statusCode() == 409 })
        val winner = body(results.single { it.statusCode() == 200 })["requestId"].asString()
        assertEquals(winner, service.current("commerce", "fifo", "owner").requestId)
        assertEquals(winner, admission(ticket)["requestId"].asString())
        assertEquals(1, service.statistics("commerce", "fifo").activeAdmissions)
    }

    @Test
    fun `same parallel requests return the identical binding without renewing TTL`() {
        val ticket = ticket()
        val results = parallel(48) { post(ticket, "claim") }
        results.forEach { assertEquals(200, it.statusCode(), it.body()) }
        assertEquals(1, results.map { body(it).toString() }.distinct().size)
        assertEquals(ticket.expiresAt.toString(), body(results.first())["expiresAt"].asString())
        assertEquals(1, service.statistics("commerce", "fifo").activeAdmissions)
    }

    @Test
    fun `changed fingerprint or request id conflicts without changing the binding`() {
        val ticket = ticket()
        claim(ticket)
        val before = snapshot()
        error(post(ticket, "claim", hash = "b".repeat(64)), 409, "CONFLICT")
        error(post(ticket, "claim", requestId = "another"), 409, "CONFLICT")
        error(post(ticket, "release", requestId = "another"), 409, "CONFLICT")
        assertEquals(before, snapshot())
    }

    @Test
    fun `release is idempotent keeps the binding and never renews terminal retention`() {
        val ticket = ticket()
        claim(ticket)
        val released = post(ticket, "release")
        assertEquals(200, released.statusCode(), released.body())
        assertEquals("RELEASED", body(released)["state"].asString())
        val before = snapshot()
        parallel(24) { post(ticket, "release") }.forEach {
            assertEquals(200, it.statusCode(), it.body())
            assertEquals(body(released), body(it))
        }
        assertEquals(before, snapshot())
        assertEquals(0, service.statistics("commerce", "fifo").activeAdmissions)
        assertNull(service.current("commerce", "fifo", "owner").admissionToken)
        assertEquals(fingerprint, admission(ticket)["requestFingerprint"].asString())
        error(post(ticket, "claim"), 409, "CONFLICT")
    }

    @Test
    fun `claimed and released tickets reject user cancellation`() {
        val ticket = ticket()
        claim(ticket)
        var before = snapshot()
        error(cancel(ticket), 409, "CONFLICT")
        assertEquals(before, snapshot())
        release(ticket)
        before = snapshot()
        error(cancel(ticket), 409, "CONFLICT")
        assertEquals(before, snapshot())
    }

    @Test
    fun `unclaimed release and canceled ticket reuse conflict`() {
        val ticket = ticket()
        val before = snapshot()
        error(post(ticket, "release"), 409, "CONFLICT")
        assertEquals(before, snapshot())
        assertEquals(200, cancel(ticket).statusCode())
        val canceled = snapshot()
        error(post(ticket, "claim"), 409, "CONFLICT")
        error(post(ticket, "release"), 409, "CONFLICT")
        assertEquals(canceled, snapshot())
    }

    @Test
    fun `real claimed TTL expires and repeated claim never grants execution again`() {
        val ticket = ticket(queue = "fast-expiry")
        claim(ticket)
        val until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
        while (service.current("commerce", "fast-expiry", "owner").state != EntryState.EXPIRED) {
            assertTrue(System.nanoTime() < until)
            Thread.sleep(10)
        }
        error(post(ticket, "claim"), 410, "EXPIRED")
        error(post(ticket, "claim", requestId = "other"), 410, "EXPIRED")
        val response = post(ticket, "release")
        assertEquals(200, response.statusCode())
        assertEquals("EXPIRED", body(response)["state"].asString())
        assertEquals("request-1", body(response)["requestId"].asString())
        assertEquals(ticket.expiresAt.toString(), body(response)["expiresAt"].asString())
        assertNull(service.current("commerce", "fast-expiry", "owner").admissionToken)
        assertEquals(0, service.statistics("commerce", "fast-expiry").activeAdmissions)
    }

    @Test
    fun `expired unclaimed ticket cannot be newly claimed or released`() {
        val ticket = ticket()
        forceExpiry(ticket)
        error(post(ticket, "claim"), 410, "EXPIRED")
        error(post(ticket, "release"), 409, "CONFLICT")
        assertFalse(admission(ticket).has("requestId"))
        assertEquals(0, service.statistics("commerce", "fifo").activeAdmissions)
    }

    @Test
    fun `late release and old record GC preserve a new current ticket`() {
        val old = ticket()
        claim(old)
        release(old)
        val fresh = ticket()
        claim(fresh, "new-request")
        val current = service.current("commerce", "fifo", "owner")
        release(old)
        assertEquals(current, service.current("commerce", "fifo", "owner"))
        forceRetention(old)
        error(post(old, "claim"), 404, "NOT_FOUND")
        error(post(old, "release"), 404, "NOT_FOUND")
        cleanup()
        assertFalse(redis.opsForHash<String, String>().hasKey(keys().admissions, old.admissionId!!))
        assertEquals(current, service.current("commerce", "fifo", "owner"))
        assertEquals(1, service.statistics("commerce", "fifo").activeAdmissions)
    }

    @Test
    fun `expired claimed replacement and late release keep the new slot`() {
        val old = ticket()
        claim(old)
        forceExpiry(old)
        val fresh = ticket()
        claim(fresh, "new-request")
        assertEquals(EntryState.EXPIRED, release(old).state)
        cleanup()
        assertEquals(fresh.admissionId, service.current("commerce", "fifo", "owner").admissionId)
        assertEquals(1, service.statistics("commerce", "fifo").activeAdmissions)
    }

    @Test
    fun `release and cleanup at the expiry boundary never double free a slot`() {
        repeat(12) { index ->
            val subject = "racer-$index"
            val ticket = ticket(subject)
            claim(ticket, subject, subject)
            forceExpiry(ticket, System.currentTimeMillis() + if (index % 2 == 0) 20 else -1)
            parallel(2) { task -> if (task == 0) release(ticket, subject, subject) else cleanup() }
            val ended = service.current("commerce", "fifo", subject)
            assertTrue(ended.state == EntryState.RELEASED || ended.state == EntryState.EXPIRED)
            assertEquals(0, service.statistics("commerce", "fifo").activeAdmissions)
            val same = post(ticket, "release", requestId = subject, subject = subject)
            assertEquals(200, same.statusCode(), same.body())
            assertEquals(ended.state.name, body(same)["state"].asString())
        }
    }

    @Test
    fun `claimed expiry and linked binding GC stay within batches of one hundred`() {
        val tickets = (0 until 103).map { i -> service.register("commerce", "batch", "owner-$i").entry }
        admit("batch"); admit("batch")
        val claimed = tickets.indices.map { i -> service.current("commerce", "batch", "owner-$i") }
        claimed.forEachIndexed { i, ticket -> claim(ticket, "request-$i", "owner-$i"); forceExpiry(ticket) }
        assertEquals(100, cleanup("batch"))
        assertEquals(3, redis.opsForZSet().zCard(keys("batch").active))
        assertEquals(3, cleanup("batch"))
        claimed.forEach { assertTrue(admission(it).has("requestFingerprint")); forceRetention(it) }
        assertEquals(100, cleanup("batch"))
        assertEquals(3, redis.opsForHash<String, String>().size(keys("batch").admissions))
        assertEquals(3, cleanup("batch"))
        assertEquals(0, redis.opsForHash<String, String>().size(keys("batch").entries))
        assertEquals(0, redis.opsForHash<String, String>().size(keys("batch").admissions))
        assertEquals("103", redis.opsForValue().get(keys("batch").sequence))
    }

    @Test
    fun `wrong owners tokens services and queues never reveal or mutate a ticket`(output: CapturedOutput) {
        val ticket = ticket()
        for (operation in listOf("claim", "release")) {
            val before = snapshot()
            error(post(ticket, operation, subject = "stranger"), 403, "FORBIDDEN")
            error(post(ticket, operation, secret = "x".repeat(43)), 403, "FORBIDDEN")
            error(post(ticket, operation, serviceToken = EVENTS_TOKEN), 403, "FORBIDDEN")
            error(post(ticket, operation, serviceToken = "bad-service-token-00000000000000000000"), 401, "UNAUTHORIZED")
            error(post(ticket.copy(queueId = "shared"), operation), 404, "NOT_FOUND")
            error(post(ticket.copy(admissionId = UUID.randomUUID().toString()), operation), 404, "NOT_FOUND")
            assertEquals(before, snapshot())
        }
        service.register("commerce", "shared", "same")
        service.register("events", "shared", "same")
        admit("shared"); store.admit("events", registry.policy("events", "shared"))
        val own = service.current("commerce", "shared", "same")
        error(post(own, "claim", subject = "same", serviceToken = EVENTS_TOKEN), 404, "NOT_FOUND")
        assertFalse(output.all.contains(assertNotNull(ticket.admissionToken)))
    }

    @Test
    fun `invalid claim and release input fails before any state change`() {
        val ticket = ticket()
        val before = snapshot()
        for (operation in listOf("claim", "release")) {
            val valid = payload(ticket)
            val mutations = listOf(
                "subjectId" to "", "subjectId" to "bad/slash", "subjectId" to false,
                "admissionToken" to "short", "admissionToken" to false,
                "requestId" to "", "requestId" to "a".repeat(129), "requestId" to 123,
            ) + if (operation == "claim") listOf("requestFingerprint" to "A".repeat(64),
                "requestFingerprint" to "a".repeat(63), "requestFingerprint" to true) else emptyList()
            for ((field, value) in mutations) error(raw(ticket, operation, valid + (field to value)), 400, "INVALID_REQUEST")
            for (field in if (operation == "claim") valid.keys else valid.keys - "requestFingerprint") {
                error(raw(ticket, operation, valid - field), 400, "INVALID_REQUEST")
                error(raw(ticket, operation, valid + (field to null)), 400, "INVALID_REQUEST")
            }
            error(post(ticket.copy(admissionId = "bad-id"), operation), 400, "INVALID_REQUEST")
            assertEquals(before, snapshot())
        }
        assertEquals(EntryState.CLAIMED, claim(ticket, "a".repeat(128)).state)
    }

    @Test
    fun `invalid Accept cannot claim or release but mixed Accept uses UTF8`() {
        val ticket = ticket()
        for (operation in listOf("claim", "release")) {
            val before = snapshot()
            for ((accept, status, code) in listOf(
                Triple("text/plain", 406, "NOT_ACCEPTABLE"),
                Triple("application/json;charset=UTF-16", 406, "NOT_ACCEPTABLE"),
                Triple("not/a/media/type", 400, "INVALID_REQUEST"),
            )) {
                error(post(ticket, operation, accept = accept), status, code)
                assertEquals(before, snapshot())
            }
            val response = post(ticket, operation,
                accept = "application/json;charset=UTF-16, application/json;charset=UTF-8;q=0.5")
            assertEquals(200, response.statusCode(), response.body())
            assertEquals("application/json;charset=UTF-8", response.headers().firstValue("Content-Type").orElseThrow())
        }
    }

    @Test
    fun `corrupted binding prevents claim release and cleanup before writes`() {
        val ticket = ticket()
        claim(ticket)
        forceExpiry(ticket)
        val admission = admission(ticket) as ObjectNode
        admission.put("requestFingerprint", "broken")
        redis.opsForHash<String, String>().put(keys().admissions, ticket.admissionId!!, mapper.writeValueAsString(admission))
        val before = snapshot()
        error(post(ticket, "claim"), 503, "UNAVAILABLE")
        error(post(ticket, "release"), 503, "UNAVAILABLE")
        assertFailsWith<DataAccessException> { cleanup() }
        assertEquals(before, snapshot())
    }

    @Test
    fun `claim racing user cancellation cannot cancel a claimed ticket`() {
        repeat(12) {
            val ticket = ticket()
            val responses = parallel(2) { task -> if (task == 0) post(ticket, "claim") else cancel(ticket) }
            val current = service.current("commerce", "fifo", "owner")
            if (current.state == EntryState.CLAIMED) {
                assertEquals(listOf(200, 409), responses.map { it.statusCode() })
                release(ticket)
            } else {
                assertEquals(EntryState.CANCELED, current.state)
                assertEquals(listOf(409, 200), responses.map { it.statusCode() })
            }
            assertEquals(0, service.statistics("commerce", "fifo").activeAdmissions)
        }
    }

    private fun keys(queue: String = "fifo") = QueueKeys("commerce", queue)
    private fun admit(queue: String = "fifo") = store.admit("commerce", registry.policy("commerce", queue))
    private fun cleanup(queue: String = "fifo") = store.cleanup("commerce", registry.policy("commerce", queue))
    private fun ticket(subject: String = "owner", queue: String = "fifo"): QueueEntry {
        service.register("commerce", queue, subject)
        admit(queue)
        return service.current("commerce", queue, subject).also { assertEquals(EntryState.ADMITTED, it.state) }
    }
    private fun claim(ticket: QueueEntry, id: String = "request-1", subject: String = "owner") =
        service.claim("commerce", ticket.queueId, subject, ticket.admissionId!!, ticket.admissionToken!!, id, fingerprint)
    private fun release(ticket: QueueEntry, id: String = "request-1", subject: String = "owner") =
        service.release("commerce", ticket.queueId, subject, ticket.admissionId!!, ticket.admissionToken!!, id)
    private fun payload(ticket: QueueEntry, subject: String = "owner", id: String = "request-1", hash: String = fingerprint,
        secret: String = ticket.admissionToken!!) =
        mapOf("subjectId" to subject, "admissionToken" to secret, "requestId" to id, "requestFingerprint" to hash)
    private fun post(ticket: QueueEntry, operation: String, subject: String = "owner", requestId: String = "request-1",
        hash: String = fingerprint, secret: String = ticket.admissionToken!!, serviceToken: String = TOKEN,
        accept: String? = null): HttpResponse<String> = raw(ticket, operation,
        payload(ticket, subject, requestId, hash, secret).filterKeys { operation == "claim" || it != "requestFingerprint" }, serviceToken, accept)
    private fun raw(ticket: QueueEntry, operation: String, payload: Map<String, Any?>, serviceToken: String = TOKEN,
        accept: String? = null): HttpResponse<String> {
        val request = HttpRequest.newBuilder(URI("http://127.0.0.1:$port/v1/queues/${ticket.queueId}/admissions/${ticket.admissionId}/$operation"))
            .timeout(Duration.ofSeconds(10)).header("Authorization", "Bearer $serviceToken")
            .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(payload)))
        if (accept != null) request.header("Accept", accept)
        return client.send(request.build(), HttpResponse.BodyHandlers.ofString())
    }
    private fun cancel(ticket: QueueEntry): HttpResponse<String> {
        val request = HttpRequest.newBuilder(URI("http://127.0.0.1:$port/v1/queues/${ticket.queueId}/entries/${ticket.entryId}/cancel"))
            .header("Authorization", "Bearer $TOKEN").header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString("{\"subjectId\":\"owner\"}"))
        return client.send(request.build(), HttpResponse.BodyHandlers.ofString())
    }
    private fun body(response: HttpResponse<String>) = mapper.readTree(response.body())
    private fun error(response: HttpResponse<String>, status: Int, code: String) {
        assertEquals(status, response.statusCode(), response.body())
        assertEquals(code, body(response)["errorCode"].asString())
        assertEquals("no-store", response.headers().firstValue("Cache-Control").orElseThrow())
        assertFalse(response.body().contains(TOKEN))
        assertFalse(response.body().contains("admissionToken"))
    }
    private fun admission(ticket: QueueEntry) = mapper.readTree(redis.opsForHash<String, String>()
        .get(keys(ticket.queueId).admissions, ticket.admissionId!!)!!)
    private fun snapshot() = keys().all.associateWith { key -> redis.dump(key)?.let { Base64.getEncoder().encodeToString(it) } }
    private fun forceExpiry(ticket: QueueEntry, at: Long = System.currentTimeMillis() - 1) {
        val k = keys(ticket.queueId)
        val entry = mapper.readTree(redis.opsForHash<String, String>().get(k.entries, ticket.entryId)!!) as ObjectNode
        val admission = admission(ticket) as ObjectNode
        entry.put("expiresAt", at); admission.put("expiresAt", at)
        redis.opsForHash<String, String>().put(k.entries, ticket.entryId, mapper.writeValueAsString(entry))
        redis.opsForHash<String, String>().put(k.admissions, ticket.admissionId!!, mapper.writeValueAsString(admission))
        redis.opsForZSet().add(k.active, ticket.admissionId, at.toDouble())
    }
    private fun forceRetention(ticket: QueueEntry) {
        val k = keys(ticket.queueId)
        val entry = mapper.readTree(redis.opsForHash<String, String>().get(k.entries, ticket.entryId)!!) as ObjectNode
        val admission = admission(ticket) as ObjectNode
        val at = System.currentTimeMillis() - 86_401_000
        for (record in listOf(entry, admission)) { record.put("terminatedAt", at); record.put("retainUntil", at + 86_400_000) }
        redis.opsForHash<String, String>().put(k.entries, ticket.entryId, mapper.writeValueAsString(entry))
        redis.opsForHash<String, String>().put(k.admissions, ticket.admissionId!!, mapper.writeValueAsString(admission))
        redis.opsForZSet().add(k.gc, "entry:${ticket.entryId}", (at + 86_400_000).toDouble())
    }
    private fun <T> parallel(count: Int, action: (Int) -> T): List<T> {
        val pool = Executors.newFixedThreadPool(minOf(count, 16))
        try { return pool.invokeAll((0 until count).map { i -> Callable { action(i) } }).map { it.get(20, TimeUnit.SECONDS) } }
        finally { pool.shutdownNow() }
    }
    companion object {
        private const val TOKEN = "test-commerce-token-not-for-production-0001"
        private const val EVENTS_TOKEN = "test-events-token-not-for-production-000002"
        @Container @JvmStatic val redisContainer = GenericContainer<Nothing>("redis:7.4-alpine").apply { withExposedPorts(6379) }
        @DynamicPropertySource @JvmStatic fun redisProperties(registry: DynamicPropertyRegistry) {
            registry.add("spring.data.redis.host", redisContainer::getHost)
            registry.add("spring.data.redis.port") { redisContainer.getMappedPort(6379) }
        }
    }
}
