package com.queueguard

import com.queueguard.api.ApiException
import com.queueguard.api.ErrorCode
import com.queueguard.redis.*
import com.queueguard.service.*
import com.queueguard.worker.AdmissionWorker
import io.micrometer.core.instrument.MeterRegistry
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.Base64
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
class AdmissionIntegrationTest {
    @LocalServerPort private var port: Int = 0
    @Autowired private lateinit var service: EntryService
    @Autowired private lateinit var store: EntryStore
    @Autowired private lateinit var registry: QueueRegistry
    @Autowired private lateinit var policies: PolicyStore
    @Autowired private lateinit var redis: StringRedisTemplate
    @Autowired private lateinit var mapper: ObjectMapper
    @Autowired private lateinit var tokens: SecureTokens
    @Autowired private lateinit var meters: MeterRegistry
    private val client = HttpClient.newHttpClient()

    @BeforeEach
    fun reset() {
        registry.allPolicies().forEach { (clientId, policy) ->
            redis.delete(QueueKeys(clientId, policy.queueId).all)
            policies.initialize(clientId, policy)
        }
    }

    @Test
    fun `FIFO approval and concurrent workers never exceed active capacity or change an existing ticket`(output: CapturedOutput) {
        val registered = (0 until 10).map { service.register("commerce", "fifo", "user-$it").entry }
        store.admit("commerce", registry.policy("commerce", "fifo")) // 승인 응답을 버린 뒤 조회한다.
        val first = service.current("commerce", "fifo", "user-0")
        assertEquals(EntryState.ADMITTED, first.state)
        assertEquals(0, parallel(24) { admit("fifo").admitted }.sum())
        val current = (0 until 10).map { service.current("commerce", "fifo", "user-$it") }
        assertEquals(registered.take(3).map { it.entryId }, current.filter { it.state == EntryState.ADMITTED }.map { it.entryId })
        assertEquals(3, redis.opsForZSet().zCard(keys("fifo").active))
        assertEquals(7, redis.opsForZSet().zCard(keys("fifo").waiting))
        assertFalse(service.register("commerce", "fifo", "user-0").created)
        assertEquals(first, service.register("commerce", "fifo", "user-0").entry)
        val response = request("fifo", "/subjects/user-0/entry")
        assertEquals(200, response.statusCode())
        assertEquals(first.admissionToken, body(response)["admissionToken"].asString())
        assertEquals("no-store", response.headers().firstValue("Cache-Control").orElseThrow())
        val secret = assertNotNull(first.admissionToken)
        assertEquals(32, Base64.getUrlDecoder().decode(secret).size)
        assertFalse(first.toString().contains(secret))
        assertFalse(output.all.contains(secret))
        assertEquals(0, current[3].waitingAhead)
    }

    @Test
    fun `parallel workers keep approvals within each Redis second window`() {
        repeat(100) { service.register("commerce", "rate-limited", "user-$it") }
        val results = parallel(48) { admit("rate-limited") }
        assertTrue(results.sumOf { it.admitted } > 0)
        results.groupBy { it.now / 1000 }.forEach { (_, values) -> assertTrue(values.sumOf { it.admitted } <= 2) }
        results.forEach { assertTrue(it.windowCount <= 2) }
        val entries = redis.opsForHash<String, String>().values(keys("rate-limited").entries).map(mapper::readTree)
        entries.filter { it["state"].asString() == "ADMITTED" }
            .groupBy { (it["expiresAt"].asLong() - 300_000) / 1000 }
            .forEach { (_, values) -> assertTrue(values.size <= 2) }
    }

    @Test
    fun `fixed window replenishes on the next Redis second`() {
        repeat(10) { service.register("commerce", "rate-limited", "user-$it") }
        await { stats("rate-limited").now % 1000 < 250 }
        val first = admit("rate-limited")
        assertEquals(2, first.admitted)
        val ticket = service.current("commerce", "rate-limited", "user-0")
        service.cancel("commerce", "rate-limited", "user-0", ticket.entryId)
        val second = admit("rate-limited")
        if (second.now / 1000 == first.now / 1000) assertEquals(0, second.admitted)
        await { stats("rate-limited").now / 1000 > first.now / 1000 }
        val next = admit("rate-limited")
        val approvals = listOf(first, second, next).filter { it.admitted > 0 }
        assertTrue(approvals.map { it.now / 1000 }.toSet().size >= 2)
        approvals.groupBy { it.now / 1000 }.forEach { (_, values) -> assertEquals(2, values.sumOf { it.admitted }) }
    }

    @Test
    fun `expired active records release capacity even before physical cleanup`() {
        val first = service.register("commerce", "fast-expiry", "first").entry
        val second = service.register("commerce", "fast-expiry", "second").entry
        admit("fast-expiry")
        val old = service.current("commerce", "fast-expiry", "first")
        forceAdmissionExpiry("fast-expiry", old.entryId)
        assertEquals(1, redis.opsForZSet().zCard(keys("fast-expiry").active))
        assertEquals(0, stats("fast-expiry").activeCount)
        assertEquals(1, admit("fast-expiry").admitted)
        assertEquals(2, redis.opsForZSet().zCard(keys("fast-expiry").active))
        val fresh = service.current("commerce", "fast-expiry", "second")
        assertEquals(second.entryId, fresh.entryId)
        assertEquals(EntryState.ADMITTED, fresh.state)
        val expired = service.current("commerce", "fast-expiry", "first")
        assertEquals(first.entryId, expired.entryId)
        assertEquals(EntryState.EXPIRED, expired.state)
        assertNull(expired.admissionToken)
        assertEquals(1, stats("fast-expiry").activeCount)
        assertEquals(ErrorCode.EXPIRED, assertFailsWith<ApiException> {
            service.cancel("commerce", "fast-expiry", "first", first.entryId)
        }.code)
        assertEquals(fresh, service.current("commerce", "fast-expiry", "second"))
    }

    @Test
    fun `real admission TTL expires without extending it on retry`() {
        service.register("commerce", "fast-expiry", "user")
        admit("fast-expiry")
        val original = service.current("commerce", "fast-expiry", "user")
        assertEquals(original, service.register("commerce", "fast-expiry", "user").entry)
        await { service.current("commerce", "fast-expiry", "user").state == EntryState.EXPIRED }
        val expired = service.current("commerce", "fast-expiry", "user")
        assertEquals(original.expiresAt, expired.expiresAt)
        assertNull(expired.admissionToken)
        val fresh = service.register("commerce", "fast-expiry", "user").entry
        assertTrue(fresh.sequence > original.sequence)
        admit("fast-expiry")
        val next = service.current("commerce", "fast-expiry", "user")
        assertNotEquals(original.admissionId, next.admissionId)
        assertNotEquals(original.admissionToken, next.admissionToken)
    }

    @Test
    fun `canceling and collecting an old admission never removes a replacement`() {
        val old = service.register("commerce", "fifo", "user").entry
        admit("fifo")
        val ticket = service.current("commerce", "fifo", "user")
        val canceled = service.cancel("commerce", "fifo", "user", old.entryId)
        assertEquals(EntryState.CANCELED, canceled.state)
        assertNull(canceled.admissionToken)
        assertEquals(0, stats("fifo").activeCount)
        service.register("commerce", "fifo", "user")
        admit("fifo")
        val fresh = service.current("commerce", "fifo", "user")
        assertEquals(canceled, service.cancel("commerce", "fifo", "user", old.entryId))
        forceRetentionDue("fifo", old.entryId)
        store.cleanup("commerce", registry.policy("commerce", "fifo"))
        assertFalse(redis.opsForHash<String, String>().hasKey(keys("fifo").admissions, ticket.admissionId!!))
        assertEquals(fresh, service.current("commerce", "fifo", "user"))
        assertEquals(1, stats("fifo").activeCount)
    }

    @Test
    fun `approval skips canceled and expired entries and scans a bounded prefix`() {
        val entries = (0 until 104).map { service.register("commerce", "batch", "user-$it").entry }
        entries.take(103).forEach { forceWaitingExpiry("batch", it.entryId) }
        service.cancel("commerce", "batch", "user-103", entries[103].entryId)
        val a = service.register("commerce", "batch", "fresh-a").entry
        val b = service.register("commerce", "batch", "fresh-b").entry
        val first = admit("batch")
        assertEquals(0, first.admitted)
        assertEquals(100, first.processed)
        assertEquals(5, redis.opsForZSet().zCard(keys("batch").waiting))
        val second = admit("batch")
        assertEquals(2, second.admitted)
        assertEquals(5, second.processed)
        assertEquals(0, redis.opsForZSet().zCard(keys("batch").waiting))
        assertEquals(a.entryId, service.current("commerce", "batch", "fresh-a").entryId)
        assertEquals(b.entryId, service.current("commerce", "batch", "fresh-b").entryId)
        assertEquals(EntryState.EXPIRED, service.current("commerce", "batch", "user-0").state)
    }

    @Test
    fun `active expiry and linked admission GC are bounded to one hundred each`() {
        val entries = (0 until 103).map { service.register("commerce", "batch", "user-$it").entry }
        assertEquals(100, admit("batch").admitted)
        assertEquals(3, admit("batch").admitted)
        entries.forEach { forceAdmissionExpiry("batch", it.entryId) }
        val policy = registry.policy("commerce", "batch")
        assertEquals(100, store.cleanup("commerce", policy))
        assertEquals(3, redis.opsForZSet().zCard(keys("batch").active))
        assertEquals(3, store.cleanup("commerce", policy))
        entries.forEach { forceRetentionDue("batch", it.entryId) }
        assertEquals(100, store.cleanup("commerce", policy))
        assertEquals(3, redis.opsForHash<String, String>().size(keys("batch").admissions))
        assertEquals(3, store.cleanup("commerce", policy))
        assertEquals(0, redis.opsForHash<String, String>().size(keys("batch").admissions))
        assertEquals(0, redis.opsForHash<String, String>().size(keys("batch").entries))
        assertEquals("103", redis.opsForValue().get(keys("batch").sequence))
    }

    @Test
    fun `candidate collisions refuse the whole batch without overwriting tickets`() {
        service.register("commerce", "fifo", "first")
        admit("fifo")
        val old = service.current("commerce", "fifo", "first")
        service.register("commerce", "fifo", "next")
        val duplicate = candidate()
        val cases = listOf(listOf(AdmissionCandidate(old.admissionId!!, tokens.newAdmissionToken())),
            listOf(duplicate, duplicate),
            listOf(duplicate, AdmissionCandidate(tokens.newId(), duplicate.admissionToken)))
        for (candidates in cases) {
            val before = snapshot("fifo")
            assertFailsWith<DataAccessException> { admit("fifo", candidates) }
            assertEquals(before, snapshot("fifo"))
        }
        assertEquals(old, service.current("commerce", "fifo", "first"))
        assertEquals(1, admit("fifo").admitted)
    }

    @Test
    fun `corruption in a later selected entry prevents every approval write`() {
        val first = service.register("commerce", "fifo", "first").entry
        val second = service.register("commerce", "fifo", "second").entry
        redis.opsForHash<String, String>().put(keys("fifo").entries, second.entryId, "{broken")
        val before = snapshot("fifo")
        assertFailsWith<DataAccessException> { admit("fifo") }
        assertEquals(before, snapshot("fifo"))
        assertEquals(EntryState.WAITING, service.current("commerce", "fifo", "first").state)
        assertEquals(0, redis.opsForZSet().zCard(keys("fifo").active))
        assertFalse(redis.hasKey(keys("fifo").rate))
    }

    @Test
    fun `future or malformed rate state refuses approvals without partial writes`() {
        service.register("commerce", "fifo", "user")
        val now = stats("fifo").now
        redis.opsForHash<String, String>().putAll(keys("fifo").rate, mapOf("window" to (now / 1000 + 100).toString(), "count" to "0"))
        var before = snapshot("fifo")
        assertFailsWith<DataAccessException> { admit("fifo") }
        assertEquals(before, snapshot("fifo"))
        redis.opsForHash<String, String>().put(keys("fifo").rate, "count", "broken")
        before = snapshot("fifo")
        assertFailsWith<DataAccessException> { admit("fifo") }
        assertEquals(before, snapshot("fifo"))
    }

    @Test
    fun `worker continues to another queue after a failure without logging tickets`(output: CapturedOutput) {
        service.register("commerce", "fifo", "bad")
        service.register("commerce", "rate-limited", "good")
        redis.delete(keys("fifo").config)
        AdmissionWorker(registry, store).admit()
        val admitted = service.current("commerce", "rate-limited", "good")
        assertEquals(EntryState.ADMITTED, admitted.state)
        assertFalse(output.all.contains(admitted.admissionToken!!))
        assertTrue(output.all.contains("Admission failed for commerce/fifo"))
    }

    @Test
    fun `queue scoped metrics expose current counts and instrument script errors`() {
        val approvedBefore = meters.get("queueguard.admissions").tags("clientId", "commerce", "queueId", "fifo").counter().count()
        val errorsBefore = meters.get("queueguard.script.errors").tags("clientId", "commerce", "queueId", "fifo").counter().count()
        service.register("commerce", "fifo", "owner")
        assertEquals(1, service.statistics("commerce", "fifo").waitingEntries)
        admit("fifo")
        val ticket = service.current("commerce", "fifo", "owner")
        val stats = service.statistics("commerce", "fifo")
        assertEquals(0, stats.waitingEntries)
        assertEquals(1, stats.activeAdmissions)
        assertEquals(approvedBefore + 1, meters.get("queueguard.admissions").tags("clientId", "commerce", "queueId", "fifo").counter().count())
        assertEquals(1.0, meters.get("queueguard.active.admissions").tags("clientId", "commerce", "queueId", "fifo").gauge().value())
        val response = request("fifo", "/metrics")
        assertEquals(200, response.statusCode())
        assertFalse(response.body().contains(ticket.admissionToken!!))
        assertEquals(403, request("fifo", "/metrics", token = EVENTS_TOKEN).statusCode())
        redis.delete(keys("fifo").config)
        assertEquals(503, request("fifo", "/metrics").statusCode())
        assertEquals(errorsBefore + 1, meters.get("queueguard.script.errors").tags("clientId", "commerce", "queueId", "fifo").counter().count())
    }

    @Test
    fun `unsupported response charset cannot change an admitted ticket or queue state`() {
        service.register("commerce", "fifo", "owner")
        admit("fifo")
        val original = service.current("commerce", "fifo", "owner")
        assertEquals(EntryState.ADMITTED, original.state)
        val before = snapshot("fifo")
        for (charset in listOf("UTF-16", "UTF-16BE", "UTF-16LE", "UTF-32", "US-ASCII", "ISO-8859-1")) {
            val accept = "application/json;charset=$charset"
            val responses = listOf(
                request("fifo", "/entries/${original.entryId}/cancel", "{\"subjectId\":\"owner\"}", accept = accept),
                request("fifo", "/subjects/owner/entry", accept = accept),
                request("fifo", "/metrics", accept = accept),
            )
            for (response in responses) {
                assertEquals(406, response.statusCode(), "$accept: ${response.body()}")
                assertEquals("NOT_ACCEPTABLE", body(response)["errorCode"].asString())
                assertEquals("no-store", response.headers().firstValue("Cache-Control").orElseThrow())
                assertFalse(response.body().contains(original.admissionToken!!))
            }
            assertEquals(before, snapshot("fifo"))
            assertEquals(original, service.current("commerce", "fifo", "owner"))
            assertEquals(1, stats("fifo").activeCount)
        }
    }

    @Test
    fun `default JSON UTF8 and wildcard Accept preserve registration ticket and metrics responses`() {
        val accepted = listOf(null, "application/json", "application/json;charset=UTF-8", "*/*",
            "application/json;charset=UTF-16, application/json;charset=UTF-8;q=0.5",
            "application/json;charset=UTF-16, */*;q=0.5")
        for ((index, accept) in accepted.withIndex()) {
            val subject = "accepted-$index"
            val registered = request("fifo", "/entries", "{\"subjectId\":\"$subject\"}", accept = accept)
            assertEquals(201, registered.statusCode(), registered.body())
            admit("fifo")
            val ticket = request("fifo", "/subjects/$subject/entry", accept = accept)
            assertEquals(200, ticket.statusCode(), ticket.body())
            assertEquals("ADMITTED", body(ticket)["state"].asString())
            val metrics = request("fifo", "/metrics", accept = accept)
            assertEquals(200, metrics.statusCode(), metrics.body())
            assertEquals(1, body(metrics)["activeAdmissions"].asLong())
            val canceled = request("fifo", "/entries/${body(ticket)["entryId"].asString()}/cancel",
                "{\"subjectId\":\"$subject\"}", accept = accept)
            assertEquals(200, canceled.statusCode(), canceled.body())
            assertEquals("CANCELED", body(canceled)["state"].asString())
            for (response in listOf(registered, ticket, metrics, canceled)) {
                assertEquals("application/json;charset=UTF-8", response.headers().firstValue("Content-Type").orElseThrow())
            }
        }
    }

    @Test
    fun `same queue and subject strings in different services have different tickets`() {
        service.register("commerce", "shared", "same")
        service.register("events", "shared", "same")
        store.admit("commerce", registry.policy("commerce", "shared"))
        store.admit("events", registry.policy("events", "shared"))
        val first = body(request("shared", "/subjects/same/entry"))
        val second = body(request("shared", "/subjects/same/entry", token = EVENTS_TOKEN))
        assertNotEquals(first["admissionId"], second["admissionId"])
        assertNotEquals(first["admissionToken"], second["admissionToken"])
        val wrong = request("shared", "/entries/${first["entryId"].asString()}/cancel", "{\"subjectId\":\"stranger\"}")
        assertEquals(403, wrong.statusCode())
        assertFalse(wrong.body().contains(first["admissionToken"].asString()))
    }

    @Test
    fun `approval and user cancellation racing never leave an occupied slot`() {
        repeat(20) { i ->
            val entry = service.register("commerce", "fifo", "user-$i").entry
            parallel(2) { task ->
                if (task == 0) admit("fifo") else service.cancel("commerce", "fifo", "user-$i", entry.entryId)
            }
            assertEquals(EntryState.CANCELED, service.current("commerce", "fifo", "user-$i").state)
            assertEquals(0, stats("fifo").activeCount)
        }
    }

    @Test
    fun `missing required state blocks approval instead of reusing lost capacity`() {
        for (missing in keys("fifo").all) {
            reset()
            service.register("commerce", "fifo", "active")
            admit("fifo")
            val canceled = service.register("commerce", "fifo", "canceled").entry
            service.cancel("commerce", "fifo", "canceled", canceled.entryId)
            service.register("commerce", "fifo", "waiting")
            assertTrue(redis.hasKey(missing), missing)
            redis.delete(missing)
            val before = snapshot("fifo")
            assertFailsWith<DataAccessException>(missing) { admit("fifo") }
            assertEquals(before, snapshot("fifo"), missing)
        }
    }

    @Test
    fun `losing all state with policy retained cannot reset a used sequence`() {
        service.register("commerce", "fifo", "original")
        redis.delete(keys("fifo").all.drop(1))
        val before = snapshot("fifo")
        assertFailsWith<DataAccessException> {
            service.register("commerce", "fifo", "replacement")
        }
        assertFailsWith<RuntimeException> { policies.initialize("commerce", registry.policy("commerce", "fifo")) }
        assertEquals(before, snapshot("fifo"))
    }

    @Test
    fun `legacy policy upgrades atomically without changing queue records`() {
        val ticket = service.register("commerce", "fifo", "owner").entry
        admit("fifo")
        val original = service.current("commerce", "fifo", "owner")
        val k = keys("fifo")
        redis.opsForHash<String, String>().put(k.config, "schemaVersion", "1")
        redis.opsForHash<String, String>().delete(k.config, "lastSequence")
        val before = snapshot("fifo").filterKeys { it != k.config }
        parallel(24) { policies.initialize("commerce", registry.policy("commerce", "fifo")) }
        assertEquals("2", redis.opsForHash<String, String>().get(k.config, "schemaVersion"))
        assertEquals(ticket.sequence.toString(), redis.opsForHash<String, String>().get(k.config, "lastSequence"))
        assertEquals(before, snapshot("fifo").filterKeys { it != k.config })
        assertEquals(original, service.current("commerce", "fifo", "owner"))
    }

    @Test
    fun `corrupt legacy state and missing sequence marker refuse startup without writes`() {
        service.register("commerce", "fifo", "owner")
        admit("fifo")
        val k = keys("fifo")
        redis.opsForHash<String, String>().put(k.config, "schemaVersion", "1")
        redis.opsForHash<String, String>().delete(k.config, "lastSequence")
        redis.delete(k.active)
        var before = snapshot("fifo")
        assertFailsWith<IllegalStateException> { policies.initialize("commerce", registry.policy("commerce", "fifo")) }
        assertEquals(before, snapshot("fifo"))
        reset()
        service.register("commerce", "fifo", "owner")
        redis.opsForHash<String, String>().delete(k.config, "lastSequence")
        before = snapshot("fifo")
        assertFailsWith<IllegalStateException> { policies.initialize("commerce", registry.policy("commerce", "fifo")) }
        assertEquals(before, snapshot("fifo"))
    }

    @Test
    fun `invalid rate headers refuse legacy migration and current initialization without writes`() {
        val invalid = listOf(
            mapOf("window" to "1000"),
            mapOf("count" to "1"),
            mapOf("window" to "1000", "count" to "1", "extra" to "1"),
            mapOf("window" to "invalid", "count" to "1"),
            mapOf("window" to "-1", "count" to "1"),
            mapOf("window" to "1.5", "count" to "1"),
            mapOf("window" to "9007199254740992", "count" to "1"),
            mapOf("window" to "1000", "count" to "invalid"),
            mapOf("window" to "1000", "count" to "-1"),
            mapOf("window" to "1000", "count" to "1.5"),
            mapOf("window" to "1000", "count" to "1000000"),
            mapOf("window" to "1000", "count" to (registry.policy("commerce", "fifo").admissionRatePerSecond + 1).toString()),
        )
        for (version in listOf("1", "2")) for (header in invalid) {
            reset()
            service.register("commerce", "fifo", "owner")
            admit("fifo")
            val original = service.current("commerce", "fifo", "owner")
            val k = keys("fifo")
            if (version == "1") {
                redis.opsForHash<String, String>().put(k.config, "schemaVersion", "1")
                redis.opsForHash<String, String>().delete(k.config, "lastSequence")
            }
            val valid = redis.opsForHash<String, String>().entries(k.rate)
            redis.delete(k.rate)
            redis.opsForHash<String, String>().putAll(k.rate, header)
            val before = snapshot("fifo")
            assertFailsWith<IllegalStateException>("version=$version header=$header") {
                policies.initialize("commerce", registry.policy("commerce", "fifo"))
            }
            assertEquals(before, snapshot("fifo"))
            redis.delete(k.rate)
            redis.opsForHash<String, String>().putAll(k.rate, valid)
            policies.initialize("commerce", registry.policy("commerce", "fifo"))
            assertEquals(original, service.current("commerce", "fifo", "owner"))
        }
    }

    private fun candidate() = AdmissionCandidate(tokens.newId(), tokens.newAdmissionToken())
    private fun admit(queueId: String, candidates: List<AdmissionCandidate> = List(100) { candidate() }) =
        store.execute("commerce", registry.policy("commerce", queueId), "ADMIT", candidates = candidates)
    private fun stats(queueId: String) = store.execute("commerce", registry.policy("commerce", queueId), "STATS")
    private fun keys(queueId: String) = QueueKeys("commerce", queueId)
    private fun snapshot(queueId: String) = keys(queueId).all.associateWith { key -> redis.dump(key)?.let { Base64.getEncoder().encodeToString(it) } }

    private fun update(queueId: String, entryId: String, edit: (ObjectNode) -> Unit): ObjectNode {
        val key = keys(queueId).entries
        val entry = mapper.readTree(redis.opsForHash<String, String>().get(key, entryId)!!) as ObjectNode
        edit(entry)
        redis.opsForHash<String, String>().put(key, entryId, mapper.writeValueAsString(entry))
        return entry
    }
    private fun forceWaitingExpiry(queueId: String, id: String) {
        val at = System.currentTimeMillis() - 1000
        update(queueId, id) { it.put("waitingExpiresAt", at) }
        redis.opsForZSet().add(keys(queueId).waitingExpirations, id, at.toDouble())
    }
    private fun forceAdmissionExpiry(queueId: String, id: String) {
        val at = System.currentTimeMillis() - 1000
        val entry = update(queueId, id) { it.put("expiresAt", at) }
        val aid = entry["admissionId"].asString()
        val admission = mapper.readTree(redis.opsForHash<String, String>().get(keys(queueId).admissions, aid)!!) as ObjectNode
        admission.put("expiresAt", at)
        redis.opsForHash<String, String>().put(keys(queueId).admissions, aid, mapper.writeValueAsString(admission))
        redis.opsForZSet().add(keys(queueId).active, aid, at.toDouble())
    }
    private fun forceRetentionDue(queueId: String, id: String) {
        val at = System.currentTimeMillis() - 86_402_000
        val entry = update(queueId, id) { it.put("terminatedAt", at); it.put("retainUntil", at + 86_400_000) }
        val aid = entry["admissionId"].asString()
        val admission = mapper.readTree(redis.opsForHash<String, String>().get(keys(queueId).admissions, aid)!!) as ObjectNode
        admission.put("terminatedAt", at); admission.put("retainUntil", at + 86_400_000)
        redis.opsForHash<String, String>().put(keys(queueId).admissions, aid, mapper.writeValueAsString(admission))
        redis.opsForZSet().add(keys(queueId).gc, "entry:$id", (at + 86_400_000).toDouble())
    }
    private fun await(condition: () -> Boolean) {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
        while (!condition()) {
            assertTrue(System.nanoTime() < deadline, "Condition timed out")
            Thread.sleep(10)
        }
    }
    private fun <T> parallel(count: Int, action: (Int) -> T): List<T> {
        val pool = Executors.newFixedThreadPool(minOf(count, 16))
        try { return pool.invokeAll((0 until count).map { i -> Callable { action(i) } }).map { it.get(20, TimeUnit.SECONDS) } }
        finally { pool.shutdownNow() }
    }
    private fun request(queue: String, path: String, payload: String? = null, token: String = TOKEN,
        accept: String? = null,
    ): HttpResponse<String> {
        val builder = HttpRequest.newBuilder(URI("http://127.0.0.1:$port/v1/queues/$queue$path"))
            .timeout(java.time.Duration.ofSeconds(10)).header("Authorization", "Bearer $token")
        if (accept != null) builder.header("Accept", accept)
        if (payload != null) builder.header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(payload))
        return client.send(builder.build(), HttpResponse.BodyHandlers.ofString())
    }
    private fun body(response: HttpResponse<String>) = mapper.readTree(response.body())
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
