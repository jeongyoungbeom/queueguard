package com.queueguard.metrics

import com.queueguard.service.QueueRegistry
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import java.util.concurrent.atomic.AtomicLong
import org.springframework.stereotype.Component

@Component
class QueueMetrics(registry: MeterRegistry, queues: QueueRegistry) {
    private class State(
        val waiting: AtomicLong, val active: AtomicLong, val approved: Counter, val errors: Counter,
    )

    private val states = queues.allPolicies().associate { (clientId, policy) ->
        val tags = arrayOf("clientId", clientId, "queueId", policy.queueId)
        val waiting = AtomicLong()
        val active = AtomicLong()
        Gauge.builder("queueguard.waiting.entries", waiting, AtomicLong::toDouble).tags(*tags).register(registry)
        Gauge.builder("queueguard.active.admissions", active, AtomicLong::toDouble).tags(*tags).register(registry)
        (clientId to policy.queueId) to State(waiting, active,
            registry.counter("queueguard.admissions", *tags), registry.counter("queueguard.script.errors", *tags))
    }

    fun record(clientId: String, queueId: String, waiting: Long, active: Long, admitted: Int) {
        states[clientId to queueId]?.let {
            it.waiting.set(waiting)
            it.active.set(active)
            if (admitted > 0) it.approved.increment(admitted.toDouble())
        }
    }

    fun failure(clientId: String, queueId: String) { states[clientId to queueId]?.errors?.increment() }
    fun observedAdmissions(clientId: String, queueId: String): Long = states.getValue(clientId to queueId).approved.count().toLong()
    fun scriptErrors(clientId: String, queueId: String): Long = states.getValue(clientId to queueId).errors.count().toLong()
}
