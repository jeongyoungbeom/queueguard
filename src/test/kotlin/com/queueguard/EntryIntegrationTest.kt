package com.queueguard

import com.queueguard.api.ApiException
import com.queueguard.api.ErrorCode
import com.queueguard.redis.EntryStore
import com.queueguard.redis.PolicyStore
import com.queueguard.redis.QueueKeys
import com.queueguard.service.*
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.UUID
import java.util.Base64
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.dao.DataAccessException
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import tools.jackson.databind.ObjectMapper
import tools.jackson.databind.node.ObjectNode

@Testcontainers
@ActiveProfiles("entries-test")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class EntryIntegrationTest {
    @LocalServerPort private var port: Int = 0
    @Autowired private lateinit var service: EntryService
    @Autowired private lateinit var store: EntryStore
    @Autowired private lateinit var registry: QueueRegistry
    @Autowired private lateinit var policies: PolicyStore
    @Autowired private lateinit var redis: StringRedisTemplate
    @Autowired private lateinit var mapper: ObjectMapper
    private val client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()
    private val keys get() = QueueKeys("commerce", "commerce-orders")

    @BeforeEach
    fun reset() {
        registry.allPolicies().forEach { (clientId, policy) ->
            redis.delete(QueueKeys(clientId, policy.queueId).all)
            policies.initialize(clientId, policy)
        }
    }

    @Test
    fun `HTTP registration retry lookup and cancellation keep the same deadline`() {
        val first = request("/entries", "{\"subjectId\":\"user-1\",\"clientId\":\"events\"}")
        assertEquals(201, first.statusCode())
        val entry = body(first)
        val again = request("/entries", "{\"subjectId\":\"user-1\"}")
        assertEquals(200, again.statusCode())
        assertEquals(entry, body(again))
        assertEquals("WAITING", entry["state"].asString())
        assertEquals(1, entry["sequence"].asLong())
        assertEquals(0, entry["waitingAhead"].asLong())
        assertEquals(2000, entry["pollAfterMillis"].asLong())
        assertEquals(entry, body(request("/subjects/user-1/entry")))
        val id = entry["entryId"].asString()
        val canceled = request("/entries/$id/cancel", "{\"subjectId\":\"user-1\"}")
        assertEquals(200, canceled.statusCode())
        assertEquals("CANCELED", body(canceled)["state"].asString())
        assertTrue(body(canceled)["waitingAhead"].isNull)
        assertTrue(body(canceled)["pollAfterMillis"].isNull)
        assertEquals(body(canceled), body(request("/entries/$id/cancel", "{\"subjectId\":\"user-1\"}")))
        assertEquals(body(canceled), body(request("/subjects/user-1/entry")))
        assertEquals("no-store", first.headers().firstValue("Cache-Control").orElseThrow())
    }

    @Test
    fun `one hundred concurrent registrations of one subject create exactly one entry`() {
        val results = parallel(100) { service.register("commerce", "commerce-orders", "same-user") }
        assertEquals(1, results.count { it.created })
        assertEquals(1, results.map { it.entry.entryId }.toSet().size)
        assertEquals(1, results.map { it.entry.waitingExpiresAt }.toSet().size)
        assertEquals("1", redis.opsForValue().get(keys.sequence))
        assertEquals(1, redis.opsForHash<String, String>().size(keys.entries))
        assertEquals(1, redis.opsForZSet().zCard(keys.waiting))
    }

    @Test
    fun `one hundred different subjects receive unique FIFO sequences`() {
        val results = parallel(100) { i -> service.register("commerce", "commerce-orders", "user-$i").entry }
        assertEquals((1L..100L).toList(), results.map { it.sequence }.sorted())
        val orderedIds = redis.opsForZSet().range(keys.waiting, 0, -1)!!.toList()
        assertEquals(results.sortedBy { it.sequence }.map { it.entryId }, orderedIds)
        val last = results.maxBy { it.sequence }
        assertEquals(99, service.current("commerce", "commerce-orders", "user-${results.indexOf(last)}").waitingAhead)
    }

    @Test
    fun `cancellation changes waiting position and late cancellation preserves a new entry`() {
        val old = service.register("commerce", "commerce-orders", "first").entry
        service.register("commerce", "commerce-orders", "second")
        assertEquals(1, service.current("commerce", "commerce-orders", "second").waitingAhead)
        service.cancel("commerce", "commerce-orders", "first", old.entryId)
        assertEquals(0, service.current("commerce", "commerce-orders", "second").waitingAhead)
        val fresh = service.register("commerce", "commerce-orders", "first").entry
        assertNotEquals(old.entryId, fresh.entryId)
        assertTrue(fresh.sequence > old.sequence)
        service.cancel("commerce", "commerce-orders", "first", old.entryId)
        assertEquals(fresh, service.current("commerce", "commerce-orders", "first"))
    }

    @Test
    fun `capacity is enforced under concurrency while existing registration still succeeds`() {
        val responses = parallel(30) { i -> request("/entries", "{\"subjectId\":\"user-$i\"}", queue = "limited") }
        assertEquals(2, responses.count { it.statusCode() == 201 })
        responses.filter { it.statusCode() != 201 }.forEach {
            assertError(it, 429, "CAPACITY_EXCEEDED")
            assertEquals("2", it.headers().firstValue("Retry-After").orElseThrow())
        }
        val first = responses.indexOfFirst { it.statusCode() == 201 }
        assertEquals(200, request("/entries", "{\"subjectId\":\"user-$first\"}", queue = "limited").statusCode())
        val id = body(responses[first])["entryId"].asString()
        service.cancel("commerce", "limited", "user-$first", id)
        assertEquals(201, request("/entries", "{\"subjectId\":\"replacement\"}", queue = "limited").statusCode())
    }

    @Test
    fun `Redis time expiry is visible on lookup without a background cleanup`() {
        val old = service.register("commerce", "short-lived", "user").entry
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
        var state: QueueEntry
        do {
            state = service.current("commerce", "short-lived", "user")
            if (state.state == EntryState.EXPIRED) break
            Thread.sleep(20)
        } while (System.nanoTime() < deadline)
        assertEquals(EntryState.EXPIRED, state.state)
        assertEquals(old.waitingExpiresAt, state.waitingExpiresAt)
        assertEquals(ErrorCode.EXPIRED, assertFailsWith<ApiException> {
            service.cancel("commerce", "short-lived", "user", old.entryId)
        }.code)
        val fresh = service.register("commerce", "short-lived", "user").entry
        assertTrue(fresh.sequence > old.sequence)
        assertNotEquals(old.entryId, fresh.entryId)
    }

    @Test
    fun `expired records do not consume capacity and waiting position can include uncleaned records`() {
        val first = service.register("commerce", "limited", "first").entry
        val second = service.register("commerce", "limited", "second").entry
        forceExpiry("limited", first.entryId)
        forceExpiry("limited", second.entryId)
        val fresh = service.register("commerce", "limited", "fresh").entry
        assertEquals(2, fresh.waitingAhead)
        assertEquals(3, redis.opsForZSet().zCard(QueueKeys("commerce", "limited").waiting))
        assertEquals(2, store.cleanup("commerce", registry.policy("commerce", "limited")))
        assertEquals(0, service.current("commerce", "limited", "fresh").waitingAhead)
    }

    @Test
    fun `reregistering expired entry before cleanup does not delete the replacement`() {
        val old = service.register("commerce", "commerce-orders", "user").entry
        forceExpiry("commerce-orders", old.entryId)
        val fresh = service.register("commerce", "commerce-orders", "user").entry
        store.cleanup("commerce", registry.policy("commerce", "commerce-orders"))
        assertEquals(fresh, service.current("commerce", "commerce-orders", "user"))
        assertEquals(ErrorCode.EXPIRED, assertFailsWith<ApiException> {
            service.cancel("commerce", "commerce-orders", "user", old.entryId)
        }.code)
        assertEquals(fresh, service.current("commerce", "commerce-orders", "user"))
    }

    @Test
    fun `bounded expiry and terminal garbage collection never delete the new subject index`() {
        val old = service.register("commerce", "commerce-orders", "user").entry
        service.cancel("commerce", "commerce-orders", "user", old.entryId)
        val beforeRetry = redis.opsForHash<String, String>().get(keys.entries, old.entryId)
        service.cancel("commerce", "commerce-orders", "user", old.entryId)
        assertEquals(beforeRetry, redis.opsForHash<String, String>().get(keys.entries, old.entryId))
        forceRetentionDue(old.entryId)
        assertError(request("/subjects/user/entry"), 404, "NOT_FOUND")
        assertError(request("/entries/${old.entryId}/cancel", "{\"subjectId\":\"user\"}"), 404, "NOT_FOUND")
        assertTrue(redis.opsForHash<String, String>().hasKey(keys.entries, old.entryId))
        val fresh = service.register("commerce", "commerce-orders", "user").entry
        assertEquals(1, store.cleanup("commerce", registry.policy("commerce", "commerce-orders")))
        assertFalse(redis.opsForHash<String, String>().hasKey(keys.entries, old.entryId))
        assertEquals(fresh, service.current("commerce", "commerce-orders", "user"))
        assertEquals(ErrorCode.NOT_FOUND, assertFailsWith<ApiException> {
            service.cancel("commerce", "commerce-orders", "user", old.entryId)
        }.code)

        service.cancel("commerce", "commerce-orders", "user", fresh.entryId)
        forceRetentionDue(fresh.entryId)
        store.cleanup("commerce", registry.policy("commerce", "commerce-orders"))
        assertFalse(redis.opsForHash<String, String>().hasKey(keys.subjects, "user"))
    }

    @Test
    fun `cleanup processes at most one hundred expiry and one hundred GC records at a time`() {
        repeat(103) { i ->
            val e = service.register("commerce", "commerce-orders", "user-$i").entry
            forceExpiry("commerce-orders", e.entryId, System.currentTimeMillis() - 86_402_000)
        }
        val policy = registry.policy("commerce", "commerce-orders")
        assertEquals(100, store.cleanup("commerce", policy))
        assertEquals(3, redis.opsForZSet().zCard(keys.waiting))
        assertEquals(100, redis.opsForZSet().zCard(keys.gc))
        assertEquals(103, store.cleanup("commerce", policy))
        assertEquals(0, redis.opsForZSet().zCard(keys.waiting))
        assertEquals(3, redis.opsForHash<String, String>().size(keys.entries))
        assertEquals(3, store.cleanup("commerce", policy))
        assertEquals(0, redis.opsForHash<String, String>().size(keys.entries))
        assertEquals(0, redis.opsForHash<String, String>().size(keys.subjects))
        assertEquals("103", redis.opsForValue().get(keys.sequence))
    }

    @Test
    fun `old terminal cleanup racing registration preserves the new entry`() {
        repeat(20) { i ->
            val old = service.register("commerce", "commerce-orders", "user-$i").entry
            service.cancel("commerce", "commerce-orders", "user-$i", old.entryId)
            forceRetentionDue(old.entryId)
            val results = parallel(2) { task ->
                if (task == 0) store.cleanup("commerce", registry.policy("commerce", "commerce-orders"))
                else service.register("commerce", "commerce-orders", "user-$i")
            }
            val fresh = (results[1] as EntryRegistration).entry
            assertEquals(fresh, service.current("commerce", "commerce-orders", "user-$i"))
        }
    }

    @Test
    fun `same subjects are isolated by both client and queue namespaces`() {
        val a = service.register("commerce", "shared", "same").entry
        val b = service.register("events", "shared", "same").entry
        val c = service.register("commerce", "commerce-orders", "same").entry
        assertEquals(3, setOf(a.entryId, b.entryId, c.entryId).size)
        service.cancel("commerce", "shared", "same", a.entryId)
        assertEquals(b, service.current("events", "shared", "same"))
        assertEquals(c, service.current("commerce", "commerce-orders", "same"))
        assertEquals(b.entryId, body(request("/subjects/same/entry", queue = "shared", token = EVENTS_TOKEN))["entryId"].asString())
    }

    @Test
    fun `invalid input missing records wrong owners and service permissions are sanitized`() {
        for (subject in listOf("", " ", "bad/slash", "bad{tag}", "x".repeat(129))) {
            assertError(request("/entries", mapper.writeValueAsString(mapOf("subjectId" to subject))), 400, "INVALID_REQUEST")
        }
        for (payload in listOf("{", "{}", "{\"subjectId\":null}")) assertError(request("/entries", payload), 400, "INVALID_REQUEST")
        assertError(request("/subjects/missing/entry"), 404, "NOT_FOUND")
        assertError(request("/subjects/bad%20subject/entry"), 400, "INVALID_REQUEST")
        assertError(request("/entries/not-a-uuid/cancel", "{\"subjectId\":\"owner\"}"), 400, "INVALID_REQUEST")
        assertError(request("/entries/${UUID.randomUUID()}/cancel", "{\"subjectId\":\"owner\"}"), 404, "NOT_FOUND")
        val entry = service.register("commerce", "commerce-orders", "owner").entry
        assertError(request("/entries/${entry.entryId}/cancel", "{\"subjectId\":\"stranger\"}"), 403, "FORBIDDEN")
        assertEquals(entry, service.current("commerce", "commerce-orders", "owner"))
        assertError(request("/entries", "{\"subjectId\":\"owner\"}", token = EVENTS_TOKEN), 403, "FORBIDDEN")
        assertError(request("/entries", "{\"subjectId\":\"owner\"}", token = null), 401, "UNAUTHORIZED")
    }

    @Test
    fun `rejected Accept never creates a registration or consumes a sequence`() {
        service.register("commerce", "commerce-orders", "existing")
        val before = snapshot()
        for ((accept, status, code) in listOf(
            Triple("text/plain", 406, "NOT_ACCEPTABLE"),
            Triple("not/a/media/type", 400, "INVALID_REQUEST"),
            Triple("application/json;charset=UTF-16", 406, "NOT_ACCEPTABLE"),
            Triple("application/json;charset=UTF-16BE", 406, "NOT_ACCEPTABLE"),
            Triple("application/json;charset=UTF-16LE", 406, "NOT_ACCEPTABLE"),
            Triple("application/json;charset=UTF-32", 406, "NOT_ACCEPTABLE"),
            Triple("application/json;charset=US-ASCII", 406, "NOT_ACCEPTABLE"),
            Triple("application/json;charset=ISO-8859-1", 406, "NOT_ACCEPTABLE"),
            Triple("application/json;charset=not-a-charset", 400, "INVALID_REQUEST"),
        )) {
            assertError(request("/entries", "{\"subjectId\":\"rejected\"}", accept = accept), status, code)
            assertEquals(before, snapshot())
            assertError(request("/subjects/rejected/entry"), 404, "NOT_FOUND")
        }
    }

    @Test
    fun `rejected Accept never cancels the waiting entry`() {
        val original = service.register("commerce", "commerce-orders", "owner").entry
        val before = snapshot()
        for ((accept, status, code) in listOf(
            Triple("text/plain", 406, "NOT_ACCEPTABLE"),
            Triple("not/a/media/type", 400, "INVALID_REQUEST"),
            Triple("application/json;charset=UTF-16", 406, "NOT_ACCEPTABLE"),
            Triple("application/json;charset=UTF-16BE", 406, "NOT_ACCEPTABLE"),
            Triple("application/json;charset=UTF-16LE", 406, "NOT_ACCEPTABLE"),
            Triple("application/json;charset=UTF-32", 406, "NOT_ACCEPTABLE"),
            Triple("application/json;charset=US-ASCII", 406, "NOT_ACCEPTABLE"),
            Triple("application/json;charset=ISO-8859-1", 406, "NOT_ACCEPTABLE"),
            Triple("application/json;charset=not-a-charset", 400, "INVALID_REQUEST"),
        )) {
            assertError(request("/entries/${original.entryId}/cancel", "{\"subjectId\":\"owner\"}", accept = accept), status, code)
            assertEquals(before, snapshot())
            assertEquals(original, service.current("commerce", "commerce-orders", "owner"))
        }
    }

    @Test
    fun `non string subject ids cannot register an entry`() {
        val before = snapshot()
        for (value in listOf("false", "true", "123", "12.5")) {
            assertError(request("/entries", "{\"subjectId\":$value}"), 400, "INVALID_REQUEST")
            assertEquals(before, snapshot())
            assertError(request("/subjects/$value/entry"), 404, "NOT_FOUND")
        }
    }

    @Test
    fun `non string cancel subjects cannot alias valid string ids`() {
        for (value in listOf("false", "true", "123", "12.5")) {
            val registration = request("/entries", mapper.writeValueAsString(mapOf("subjectId" to value)))
            assertEquals(201, registration.statusCode())
            val id = body(registration)["entryId"].asString()
            val before = snapshot()
            assertError(request("/entries/$id/cancel", "{\"subjectId\":$value}"), 400, "INVALID_REQUEST")
            assertEquals(before, snapshot())
            assertEquals("WAITING", body(request("/subjects/$value/entry"))["state"].asString())
            val canceled = request("/entries/$id/cancel", mapper.writeValueAsString(mapOf("subjectId" to value)))
            assertEquals(200, canceled.statusCode())
            assertEquals("CANCELED", body(canceled)["state"].asString())
        }
    }

    @Test
    fun `sequence remains exact near the double precision limit and refuses exhaustion`() {
        redis.opsForValue().set(keys.sequence, "9007199254740989")
        redis.opsForHash<String, String>().put(keys.config, "lastSequence", "9007199254740989")
        val first = service.register("commerce", "commerce-orders", "one").entry
        val last = service.register("commerce", "commerce-orders", "two").entry
        assertEquals(9007199254740990L, first.sequence)
        assertEquals(9007199254740991L, last.sequence)
        assertEquals(1, last.waitingAhead)
        assertEquals("9007199254740991", redis.opsForValue().get(keys.sequence))
        assertError(request("/entries", "{\"subjectId\":\"three\"}"), 503, "UNAVAILABLE")
        assertEquals(2, redis.opsForHash<String, String>().size(keys.entries))
        assertEquals(200, request("/entries", "{\"subjectId\":\"two\"}").statusCode())
    }

    @Test
    fun `wrong key types missing policy and broken subject records never create replacements`() {
        val entry = service.register("commerce", "commerce-orders", "owner").entry
        val beforeSequence = redis.opsForValue().get(keys.sequence)
        val beforeEntries = redis.opsForHash<String, String>().entries(keys.entries)
        redis.delete(keys.sequence)
        assertError(request("/entries", "{\"subjectId\":\"new\"}"), 503, "UNAVAILABLE")
        assertFalse(redis.hasKey(keys.sequence))
        assertEquals(beforeEntries, redis.opsForHash<String, String>().entries(keys.entries))
        redis.opsForValue().set(keys.sequence, beforeSequence!!)
        redis.delete(keys.gc)
        redis.opsForValue().set(keys.gc, "wrong-type")
        assertError(request("/entries", "{\"subjectId\":\"new\"}"), 503, "UNAVAILABLE")
        assertEquals(beforeSequence, redis.opsForValue().get(keys.sequence))
        assertEquals(beforeEntries, redis.opsForHash<String, String>().entries(keys.entries))
        redis.delete(keys.gc)
        redis.opsForHash<String, String>().delete(keys.entries, entry.entryId)
        assertError(request("/entries", "{\"subjectId\":\"owner\"}"), 503, "UNAVAILABLE")
        assertEquals(beforeSequence, redis.opsForValue().get(keys.sequence))
        redis.delete(keys.config)
        assertError(request("/subjects/owner/entry"), 503, "UNAVAILABLE")
        assertFalse(redis.hasKey(keys.config))
    }

    @Test
    fun `corrupted later record prevents any writes in an expiry batch`() {
        val first = service.register("commerce", "commerce-orders", "first").entry
        val second = service.register("commerce", "commerce-orders", "second").entry
        forceExpiry("commerce-orders", first.entryId, System.currentTimeMillis() - 2000)
        forceExpiry("commerce-orders", second.entryId, System.currentTimeMillis() - 1000)
        val firstBefore = redis.opsForHash<String, String>().get(keys.entries, first.entryId)
        redis.opsForHash<String, String>().put(keys.entries, second.entryId, "{broken")
        assertFailsWith<DataAccessException> {
            store.cleanup("commerce", registry.policy("commerce", "commerce-orders"))
        }
        assertEquals(firstBefore, redis.opsForHash<String, String>().get(keys.entries, first.entryId))
        assertEquals(2, redis.opsForZSet().zCard(keys.waiting))
        assertEquals(0, redis.opsForZSet().zCard(keys.gc))
    }

    private fun forceExpiry(queueId: String, id: String, at: Long = System.currentTimeMillis() - 1000) {
        val k = QueueKeys("commerce", queueId)
        val raw = redis.opsForHash<String, String>().get(k.entries, id)!!
        val entry = mapper.readTree(raw) as ObjectNode
        entry.put("waitingExpiresAt", at)
        redis.opsForHash<String, String>().put(k.entries, id, mapper.writeValueAsString(entry))
        redis.opsForZSet().add(k.waitingExpirations, id, at.toDouble())
    }

    private fun forceRetentionDue(id: String) {
        val raw = redis.opsForHash<String, String>().get(keys.entries, id)!!
        val entry = mapper.readTree(raw) as ObjectNode
        entry.put("terminatedAt", System.currentTimeMillis() - 86_402_000)
        entry.put("retainUntil", entry["terminatedAt"].asLong() + 86_400_000)
        redis.opsForHash<String, String>().put(keys.entries, id, mapper.writeValueAsString(entry))
        redis.opsForZSet().add(keys.gc, "entry:$id", entry["retainUntil"].asDouble())
    }

    private fun <T> parallel(count: Int, action: (Int) -> T): List<T> {
        val pool = Executors.newFixedThreadPool(minOf(count, 32))
        try {
            return pool.invokeAll((0 until count).map { i -> Callable { action(i) } })
                .map { it.get(20, TimeUnit.SECONDS) }
        } finally {
            pool.shutdownNow()
        }
    }

    private fun request(
        path: String, payload: String? = null, queue: String = "commerce-orders", token: String? = TOKEN,
        accept: String? = null,
    ): HttpResponse<String> {
        val builder = HttpRequest.newBuilder(URI("http://127.0.0.1:$port/v1/queues/$queue$path"))
            .timeout(Duration.ofSeconds(10))
        if (token != null) builder.header("Authorization", "Bearer $token")
        if (accept != null) builder.header("Accept", accept)
        if (payload != null) builder.header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(payload))
        return client.send(builder.build(), HttpResponse.BodyHandlers.ofString())
    }

    private fun snapshot(): Map<String, String?> = keys.all.associateWith { key ->
        redis.dump(key)?.let { Base64.getEncoder().encodeToString(it) }
    }

    private fun body(response: HttpResponse<String>) = mapper.readTree(response.body())

    private fun assertError(response: HttpResponse<String>, status: Int, code: String) {
        assertEquals(status, response.statusCode(), response.body())
        assertEquals(code, body(response)["errorCode"].asString())
        assertFalse(response.body().contains(TOKEN))
        assertEquals("no-store", response.headers().firstValue("Cache-Control").orElseThrow())
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
