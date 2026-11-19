package com.queueguard.redis

import com.queueguard.config.QueuePolicy
import com.queueguard.service.QueueRegistry
import org.springframework.beans.factory.InitializingBean
import org.springframework.core.io.ClassPathResource
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.script.DefaultRedisScript
import org.springframework.stereotype.Component

@Component
class PolicyStore(private val redis: StringRedisTemplate, private val registry: QueueRegistry) : InitializingBean {
    private val initialize = DefaultRedisScript<Long>().apply {
        setLocation(ClassPathResource("lua/initialize-policy.lua"))
        resultType = Long::class.javaObjectType
    }

    override fun afterPropertiesSet() {
        registry.allPolicies().forEach { (clientId, policy) -> initialize(clientId, policy) }
    }

    fun initialize(clientId: String, policy: QueuePolicy) {
        val fields = linkedMapOf(
            "schemaVersion" to "2", "enabled" to "true",
            "maxActiveAdmissions" to policy.maxActiveAdmissions.toString(),
            "admissionRatePerSecond" to policy.admissionRatePerSecond.toString(),
            "admissionTtlSeconds" to policy.admissionTtlSeconds.toString(),
            "maxWaitingEntries" to policy.maxWaitingEntries.toString(),
            "waitingTtlSeconds" to policy.waitingTtlSeconds.toString(),
            "terminalRetentionSeconds" to policy.terminalRetentionSeconds.toString(),
        )
        val result = redis.execute(initialize, QueueKeys(clientId, policy.queueId).all,
            *fields.flatMap { listOf(it.key, it.value) }.toTypedArray())
        check(result == 0L || result == 1L || result == 2L) { "Queue policy mismatch or corrupted state; initialization refused" }
    }
}
