package com.queueguard.service

import java.time.Instant

const val SUBJECT_ID_PATTERN = "[A-Za-z0-9][A-Za-z0-9._:@-]{0,127}"

const val UUID_V4_PATTERN = "[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"
const val ADMISSION_TOKEN_PATTERN = "[A-Za-z0-9_-]{43}"
const val FINGERPRINT_PATTERN = "[0-9a-f]{64}"

enum class EntryState { WAITING, ADMITTED, CLAIMED, RELEASED, CANCELED, EXPIRED }

data class QueueEntry(
    val queueId: String,
    val entryId: String,
    val state: EntryState,
    val sequence: Long,
    val waitingExpiresAt: Instant,
    val waitingAhead: Long?,
    val pollAfterMillis: Long?,
    val admissionId: String? = null,
    val admissionToken: String? = null,
    val expiresAt: Instant? = null,
    val requestId: String? = null,
) {
    override fun toString() = "QueueEntry(queueId=$queueId, entryId=$entryId, state=$state, sequence=$sequence)"
}

data class EntryRegistration(val created: Boolean, val entry: QueueEntry)

data class QueueStatistics(
    val queueId: String, val waitingEntries: Long, val activeAdmissions: Long,
    val admissionsThisSecond: Long, val observedAdmissions: Long, val scriptErrors: Long,
    val observedAt: Instant,
)

data class AdmissionResult(
    val queueId: String, val admissionId: String, val state: EntryState,
    val requestId: String, val expiresAt: Instant,
)
