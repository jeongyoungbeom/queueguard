package com.queueguard.worker

import com.queueguard.redis.EntryStore
import com.queueguard.service.QueueRegistry
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
@ConditionalOnProperty(prefix = "queueguard.admission", name = ["enabled"], matchIfMissing = true)
class AdmissionWorker(private val queues: QueueRegistry, private val store: EntryStore) {
    private val logger = LoggerFactory.getLogger(javaClass)

    // shortcut: 회차당 선두 100건만 검사, 승인/정리 적체가 생기면 S9에서 처리량을 조정한다.
    @Scheduled(fixedDelay = 100)
    fun admit() {
        queues.allPolicies().forEach { (clientId, policy) ->
            try {
                store.admit(clientId, policy)
            } catch (e: RuntimeException) {
                logger.warn("Admission failed for {}/{} ({})", clientId, policy.queueId, e.javaClass.simpleName)
            }
        }
    }
}
