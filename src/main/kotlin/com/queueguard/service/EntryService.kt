package com.queueguard.service

import com.queueguard.api.ApiException
import com.queueguard.api.ErrorCode
import com.queueguard.redis.EntryStore
import com.queueguard.metrics.QueueMetrics
import java.time.Instant
import org.springframework.stereotype.Service

@Service
class EntryService(
    private val registry: QueueRegistry, private val store: EntryStore, private val tokens: SecureTokens,
    private val metrics: QueueMetrics,
) {
    fun register(clientId: String, queueId: String, subjectId: String): EntryRegistration =
        operate(clientId, queueId, subjectId, "REGISTER", tokens.newId())

    fun current(clientId: String, queueId: String, subjectId: String): QueueEntry =
        operate(clientId, queueId, subjectId, "GET").entry

    fun statistics(clientId: String, queueId: String): QueueStatistics {
        val result = store.execute(clientId, registry.policy(clientId, queueId), "STATS")
        return QueueStatistics(queueId, result.waitingCount, result.activeCount, result.windowCount,
            metrics.observedAdmissions(clientId, queueId), metrics.scriptErrors(clientId, queueId),
            Instant.ofEpochMilli(result.now))
    }

    fun cancel(clientId: String, queueId: String, subjectId: String, entryId: String): QueueEntry {
        if (!entryId.matches(Regex(UUID_V4_PATTERN))) {
            throw ApiException(ErrorCode.INVALID_REQUEST)
        }
        return operate(clientId, queueId, subjectId, "CANCEL", entryId).entry
    }

    fun claim(
        clientId: String, queueId: String, subjectId: String, admissionId: String,
        admissionToken: String, requestId: String, requestFingerprint: String,
    ): AdmissionResult = admission(clientId, queueId, subjectId, admissionId, admissionToken,
        requestId, "CLAIM", requestFingerprint)

    fun release(
        clientId: String, queueId: String, subjectId: String, admissionId: String,
        admissionToken: String, requestId: String,
    ): AdmissionResult = admission(clientId, queueId, subjectId, admissionId, admissionToken, requestId, "RELEASE")

    private fun admission(
        clientId: String, queueId: String, subjectId: String, admissionId: String,
        admissionToken: String, requestId: String, operation: String, fingerprint: String = "",
    ): AdmissionResult {
        val policy = registry.policy(clientId, queueId)
        if (!subjectId.matches(Regex(SUBJECT_ID_PATTERN)) || !requestId.matches(Regex(SUBJECT_ID_PATTERN))
            || !admissionId.matches(Regex(UUID_V4_PATTERN)) || !admissionToken.matches(Regex(ADMISSION_TOKEN_PATTERN))
            || (operation == "CLAIM" && !fingerprint.matches(Regex(FINGERPRINT_PATTERN)))) {
            throw ApiException(ErrorCode.INVALID_REQUEST)
        }
        val entry = store.execute(clientId, policy, operation, subjectId, admissionId = admissionId,
            admissionToken = admissionToken, requestId = requestId, fingerprint = fingerprint).entry
            ?: throw ApiException(ErrorCode.UNAVAILABLE)
        return AdmissionResult(queueId, entry.admissionId ?: throw ApiException(ErrorCode.UNAVAILABLE),
            EntryState.valueOf(entry.state), entry.requestId ?: throw ApiException(ErrorCode.UNAVAILABLE),
            Instant.ofEpochMilli(entry.expiresAt ?: throw ApiException(ErrorCode.UNAVAILABLE)))
    }

    private fun operate(
        clientId: String, queueId: String, subjectId: String, operation: String, entryId: String = "",
    ): EntryRegistration {
        val policy = registry.policy(clientId, queueId)
        if (!subjectId.matches(Regex(SUBJECT_ID_PATTERN))) throw ApiException(ErrorCode.INVALID_REQUEST)
        val result = store.execute(clientId, policy, operation, subjectId, entryId)
        val entry = result.entry ?: throw ApiException(ErrorCode.UNAVAILABLE)
        val state = EntryState.valueOf(entry.state)
        return EntryRegistration(result.created, QueueEntry(queueId, entry.entryId, state, entry.sequence.toLong(),
            Instant.ofEpochMilli(entry.waitingExpiresAt), result.waitingAhead,
            if (state == EntryState.WAITING) minOf(2000L, policy.admissionTtlSeconds * 500L) else null, entry.admissionId,
            entry.admissionToken, entry.expiresAt?.let(Instant::ofEpochMilli), entry.requestId))
    }
}
