package com.queueguard.api

import com.queueguard.service.EntryService
import com.queueguard.service.ServicePrincipal
import com.queueguard.service.SUBJECT_ID_PATTERN
import com.queueguard.service.ADMISSION_TOKEN_PATTERN
import com.queueguard.service.FINGERPRINT_PATTERN
import jakarta.servlet.http.HttpServletResponse
import jakarta.validation.Valid
import jakarta.validation.constraints.Pattern
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.security.core.annotation.AuthenticationPrincipal
import org.springframework.web.bind.annotation.*

data class SubjectRequest(@field:Pattern(regexp = SUBJECT_ID_PATTERN) val subjectId: String)

// 토큰을 포함하므로 자동 toString을 생성하지 않는다.
class ClaimRequest(
    @field:Pattern(regexp = SUBJECT_ID_PATTERN) val subjectId: String,
    @field:Pattern(regexp = ADMISSION_TOKEN_PATTERN) val admissionToken: String,
    @field:Pattern(regexp = SUBJECT_ID_PATTERN) val requestId: String,
    @field:Pattern(regexp = FINGERPRINT_PATTERN) val requestFingerprint: String,
)

class ReleaseRequest(
    @field:Pattern(regexp = SUBJECT_ID_PATTERN) val subjectId: String,
    @field:Pattern(regexp = ADMISSION_TOKEN_PATTERN) val admissionToken: String,
    @field:Pattern(regexp = SUBJECT_ID_PATTERN) val requestId: String,
)

@RestController
@RequestMapping("/v1/queues/{queueId}", produces = ["application/json;charset=UTF-8"])
class EntryController(private val entries: EntryService) {
    // 여러 Accept 후보가 있어도 응답 변환기가 다른 charset을 다시 선택하지 않게 한다.
    @ModelAttribute
    fun responseEncoding(response: HttpServletResponse) {
        response.contentType = "application/json;charset=UTF-8"
    }

    @PostMapping("/admissions/{admissionId}/claim")
    fun claim(
        @AuthenticationPrincipal principal: ServicePrincipal, @PathVariable queueId: String,
        @PathVariable admissionId: String, @Valid @RequestBody request: ClaimRequest,
    ) = entries.claim(principal.clientId, queueId, request.subjectId, admissionId,
        request.admissionToken, request.requestId, request.requestFingerprint)

    @PostMapping("/admissions/{admissionId}/release")
    fun release(
        @AuthenticationPrincipal principal: ServicePrincipal, @PathVariable queueId: String,
        @PathVariable admissionId: String, @Valid @RequestBody request: ReleaseRequest,
    ) = entries.release(principal.clientId, queueId, request.subjectId, admissionId,
        request.admissionToken, request.requestId)

    @GetMapping("/metrics")
    fun metrics(@AuthenticationPrincipal principal: ServicePrincipal, @PathVariable queueId: String) =
        entries.statistics(principal.clientId, queueId)

    @PostMapping("/entries")
    fun register(
        @AuthenticationPrincipal principal: ServicePrincipal, @PathVariable queueId: String,
        @Valid @RequestBody request: SubjectRequest,
    ) = entries.register(principal.clientId, queueId, request.subjectId).let {
        ResponseEntity.status(if (it.created) HttpStatus.CREATED else HttpStatus.OK).body(it.entry)
    }

    @GetMapping("/subjects/{subjectId}/entry")
    fun current(
        @AuthenticationPrincipal principal: ServicePrincipal, @PathVariable queueId: String,
        @PathVariable subjectId: String,
    ) = entries.current(principal.clientId, queueId, subjectId)

    @PostMapping("/entries/{entryId}/cancel")
    fun cancel(
        @AuthenticationPrincipal principal: ServicePrincipal, @PathVariable queueId: String,
        @PathVariable entryId: String, @Valid @RequestBody request: SubjectRequest,
    ) = entries.cancel(principal.clientId, queueId, request.subjectId, entryId)
}
