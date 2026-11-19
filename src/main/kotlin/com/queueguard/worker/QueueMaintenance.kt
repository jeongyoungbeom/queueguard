package com.queueguard.worker

import com.queueguard.redis.EntryStore
import com.queueguard.service.QueueRegistry
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
@ConditionalOnProperty(prefix = "queueguard.maintenance", name = ["enabled"], matchIfMissing = true)
class QueueMaintenance(private val registry: QueueRegistry, private val store: EntryStore) {
    private val logger = LoggerFactory.getLogger(javaClass)

    // shortcut: 회차당 만료·GC 각각 100건, 정리 적체가 생기면 S9에서 처리량을 조정한다.
    @Scheduled(fixedDelay = 1000)
    fun cleanup() {
        registry.allPolicies().forEach { (clientId, policy) ->
            try {
                store.cleanup(clientId, policy)
            } catch (e: RuntimeException) {
                logger.warn("Queue cleanup failed for {}/{} ({})", clientId, policy.queueId, e.javaClass.simpleName)
            }
        }
    }
}
