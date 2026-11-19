package com.queueguard.redis

import com.queueguard.api.ApiException
import com.queueguard.api.ErrorCode
import com.queueguard.config.QueuePolicy
import com.queueguard.metrics.QueueMetrics
import com.queueguard.service.SecureTokens
import org.springframework.core.io.ClassPathResource
import org.springframework.dao.DataAccessException
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.script.DefaultRedisScript
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper

internal class StoredEntry(
    val entryId: String, val state: String, val sequence: String, val waitingExpiresAt: Long,
    val admissionId: String? = null, val admissionToken: String? = null, val expiresAt: Long? = null,
    val requestId: String? = null,
)

internal class AdmissionCandidate(val admissionId: String, val admissionToken: String)

internal data class EntryScriptResult(
    val code: String, val created: Boolean = false, val entry: StoredEntry? = null,
    val waitingAhead: Long? = null, val processed: Int = 0, val admitted: Int = 0,
    val waitingCount: Long = 0, val activeCount: Long = 0, val windowCount: Long = 0, val now: Long = 0,
)

@Component
class EntryStore(
    private val redis: StringRedisTemplate, private val mapper: ObjectMapper,
    private val tokens: SecureTokens, private val metrics: QueueMetrics,
) {
    private val script = DefaultRedisScript<String>().apply {
        setLocation(ClassPathResource("lua/entry-operation.lua"))
        resultType = String::class.java
    }

    internal fun execute(
        clientId: String, policy: QueuePolicy, operation: String, subjectId: String = "", entryId: String = "",
        candidates: List<AdmissionCandidate> = emptyList(),
        admissionId: String = "", admissionToken: String = "", requestId: String = "", fingerprint: String = "",
    ): EntryScriptResult {
        val raw = try {
            redis.execute(script, QueueKeys(clientId, policy.queueId).all,
                operation, subjectId, entryId, policy.maxWaitingEntries.toString(),
                policy.waitingTtlSeconds.toString(), policy.terminalRetentionSeconds.toString(),
                policy.maxActiveAdmissions.toString(), policy.admissionRatePerSecond.toString(),
                policy.admissionTtlSeconds.toString(), mapper.writeValueAsString(candidates),
                admissionId, admissionToken, requestId, fingerprint)
        } catch (e: DataAccessException) {
            metrics.failure(clientId, policy.queueId)
            throw e
        } ?: run {
            metrics.failure(clientId, policy.queueId)
            throw ApiException(ErrorCode.UNAVAILABLE)
        }
        val result = mapper.readValue(raw, EntryScriptResult::class.java)
        metrics.record(clientId, policy.queueId, result.waitingCount, result.activeCount, result.admitted)
        if (result.code != "OK") throw ApiException(ErrorCode.valueOf(result.code))
        return result
    }

    fun admit(clientId: String, policy: QueuePolicy): Int = execute(clientId, policy, "ADMIT",
        candidates = List(100) { AdmissionCandidate(tokens.newId(), tokens.newAdmissionToken()) }).admitted

    fun cleanup(clientId: String, policy: QueuePolicy): Int = execute(clientId, policy, "CLEANUP").processed
}
