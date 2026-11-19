package com.queueguard.service

import com.queueguard.api.ApiException
import com.queueguard.api.ErrorCode
import com.queueguard.config.IDENTIFIER_PATTERN
import com.queueguard.config.QueueGuardProperties
import com.queueguard.config.QueuePolicy
import java.security.MessageDigest
import org.springframework.stereotype.Component

data class ServicePrincipal(val clientId: String)

@Component
class QueueRegistry(properties: QueueGuardProperties) {
    private val policies = properties.clients.associate { c -> c.clientId to c.queues.associateBy { it.queueId } }
    private val credentials: List<Pair<ByteArray, ServicePrincipal>>

    init {
        require(properties.clients.map { it.clientId }.distinct().size == properties.clients.size) {
            "Duplicate clientId in queueguard configuration"
        }
        for (client in properties.clients) {
            require(client.token.matches(Regex("[A-Za-z0-9_-]{32,256}"))) {
                "Service token must contain 32..256 base64url characters"
            }
            require(client.queues.map { it.queueId }.distinct().size == client.queues.size) {
                "Duplicate queueId in client configuration"
            }
        }
        credentials = properties.clients.map { digest(it.token) to ServicePrincipal(it.clientId) }
        require(credentials.map { it.first.toList() }.distinct().size == credentials.size) {
            "Service tokens must be distinct"
        }
    }

    fun authenticate(token: String): ServicePrincipal? {
        val hash = digest(token)
        return credentials.singleOrNull { MessageDigest.isEqual(it.first, hash) }?.second
    }

    fun policy(clientId: String, queueId: String): QueuePolicy {
        if (!queueId.matches(Regex(IDENTIFIER_PATTERN))) throw ApiException(ErrorCode.INVALID_REQUEST)
        policies[clientId]?.get(queueId)?.let { return it }
        if (policies.values.any { queueId in it }) throw ApiException(ErrorCode.FORBIDDEN)
        throw ApiException(ErrorCode.NOT_FOUND)
    }

    fun allPolicies(): List<Pair<String, QueuePolicy>> =
        policies.flatMap { (clientId, queues) -> queues.values.map { clientId to it } }

    private fun digest(value: String): ByteArray =
        MessageDigest.getInstance("SHA-256").digest(value.toByteArray(Charsets.UTF_8))
}
