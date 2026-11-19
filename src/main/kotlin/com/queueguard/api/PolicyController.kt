package com.queueguard.api

import com.queueguard.config.QueuePolicy
import com.queueguard.service.QueueRegistry
import com.queueguard.service.ServicePrincipal
import org.springframework.security.core.annotation.AuthenticationPrincipal
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RestController

data class PolicyResponse(val clientId: String, val policy: QueuePolicy)

@RestController
class PolicyController(private val registry: QueueRegistry) {
    @GetMapping("/v1/queues/{queueId}/policy")
    fun policy(@AuthenticationPrincipal principal: ServicePrincipal, @PathVariable queueId: String) =
        PolicyResponse(principal.clientId, registry.policy(principal.clientId, queueId))
}
