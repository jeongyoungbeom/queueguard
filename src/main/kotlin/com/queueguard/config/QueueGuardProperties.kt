package com.queueguard.config

import jakarta.validation.Valid
import jakarta.validation.constraints.Max
import jakarta.validation.constraints.Min
import jakarta.validation.constraints.NotEmpty
import jakarta.validation.constraints.Pattern
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.validation.annotation.Validated

const val IDENTIFIER_PATTERN = "[a-z][a-z0-9-]{0,63}"

@Validated
@ConfigurationProperties("queueguard")
class QueueGuardProperties(
    @field:Valid @field:NotEmpty val clients: List<ClientConfiguration> = emptyList(),
)

// 자격 증명을 포함하므로 자동 toString을 생성하지 않는다.
class ClientConfiguration(
    @field:Pattern(regexp = IDENTIFIER_PATTERN) val clientId: String,
    val token: String = "",
    @field:Valid @field:NotEmpty val queues: List<QueuePolicy>,
)

data class QueuePolicy(
    @field:Pattern(regexp = IDENTIFIER_PATTERN) val queueId: String,
    @field:Min(1) @field:Max(1_000_000) val maxActiveAdmissions: Int = 100,
    @field:Min(1) @field:Max(100_000) val admissionRatePerSecond: Int = 20,
    @field:Min(1) @field:Max(86_400) val admissionTtlSeconds: Int = 300,
    @field:Min(1) @field:Max(10_000_000) val maxWaitingEntries: Int = 10_000,
    @field:Min(1) @field:Max(604_800) val waitingTtlSeconds: Int = 3600,
    @field:Min(86_400) @field:Max(2_592_000) val terminalRetentionSeconds: Int = 86_400,
)
