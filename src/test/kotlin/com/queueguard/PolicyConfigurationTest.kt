package com.queueguard

import com.queueguard.config.*
import com.queueguard.redis.QueueKeys
import com.queueguard.service.QueueRegistry
import com.queueguard.service.SecureTokens
import jakarta.validation.Validation
import kotlin.test.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.boot.test.context.TestConfiguration
import java.util.Base64
import java.util.UUID

class PolicyConfigurationTest {
    private val token = "unit-test-token-that-must-not-be-logged-0001"

    @Test
    fun `bound valid configuration starts and missing or invalid credentials fail closed`() {
        val runner = ApplicationContextRunner().withUserConfiguration(PropertiesConfig::class.java)
            .withBean(QueueRegistry::class.java)
        val values = arrayOf("queueguard.clients[0].client-id=commerce", "queueguard.clients[0].token=$token",
            "queueguard.clients[0].queues[0].queue-id=orders")
        runner.withPropertyValues(*values).run { assertNull(it.startupFailure) }
        runner.run { assertNotNull(it.startupFailure) }
        runner.withPropertyValues(*values, "queueguard.clients[0].token=short-sensitive-token").run {
            assertNotNull(it.startupFailure)
            assertFalse(it.startupFailure!!.stackTraceToString().contains("short-sensitive-token"))
        }
        runner.withPropertyValues(*values, "queueguard.clients[0].queues[0].max-active-admissions=0").run {
            assertNotNull(it.startupFailure)
        }
    }

    @Test
    fun `policy bounds and identifiers are validated`() {
        val valid = QueuePolicy("orders")
        val invalid = listOf(
            valid.copy(queueId = "bad{slot}"), valid.copy(queueId = "A"), valid.copy(queueId = "a".repeat(65)),
            valid.copy(maxActiveAdmissions = 0), valid.copy(maxActiveAdmissions = -1), valid.copy(maxActiveAdmissions = 1_000_001),
            valid.copy(admissionRatePerSecond = 0), valid.copy(admissionRatePerSecond = 100_001),
            valid.copy(admissionTtlSeconds = 0), valid.copy(admissionTtlSeconds = 86_401),
            valid.copy(maxWaitingEntries = 0), valid.copy(maxWaitingEntries = 10_000_001),
            valid.copy(waitingTtlSeconds = 0), valid.copy(waitingTtlSeconds = 604_801),
            valid.copy(terminalRetentionSeconds = 86_399), valid.copy(terminalRetentionSeconds = 2_592_001),
        )
        Validation.buildDefaultValidatorFactory().use { factory ->
            assertTrue(factory.validator.validate(valid).isEmpty())
            invalid.forEach { assertTrue(factory.validator.validate(it).isNotEmpty(), it.toString()) }
        }
    }

    @Test
    fun `duplicate clients queues and tokens are rejected without credential disclosure`() {
        val first = ClientConfiguration("commerce", token, listOf(QueuePolicy("orders")))
        val cases = listOf(
            listOf(first, first),
            listOf(ClientConfiguration("commerce", token, listOf(QueuePolicy("orders"), QueuePolicy("orders")))),
            listOf(first, ClientConfiguration("events", token, listOf(QueuePolicy("signup")))),
        )
        cases.forEach { clients ->
            val error = assertFailsWith<IllegalArgumentException> { QueueRegistry(QueueGuardProperties(clients)) }
            assertFalse(error.stackTraceToString().contains(token))
        }
        assertFalse(first.toString().contains(token))
    }

    @ParameterizedTest
    @ValueSource(strings = ["bad{tag}", "bad:tag", "", "UPPER", "a/b"])
    fun `key builder rejects namespace manipulation`(id: String) {
        assertFailsWith<IllegalArgumentException> { QueueKeys(id, "orders") }
        assertFailsWith<IllegalArgumentException> { QueueKeys("commerce", id) }
    }

    @Test
    fun `all queue keys share a hash tag and token generator uses 256 bits`() {
        assertTrue(QueueKeys("commerce", "orders").all.all { it.startsWith("{qg:commerce:orders}:") })
        val generator = SecureTokens()
        val tokens = List(100) { generator.newAdmissionToken() }
        assertEquals(100, tokens.toSet().size)
        tokens.forEach { assertEquals(32, Base64.getUrlDecoder().decode(it).size) }
        UUID.fromString(generator.newId())
    }

    @TestConfiguration(proxyBeanMethods = false)
    @EnableConfigurationProperties(QueueGuardProperties::class)
    class PropertiesConfig
}
