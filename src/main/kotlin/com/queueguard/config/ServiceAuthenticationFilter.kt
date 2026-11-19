package com.queueguard.config

import com.queueguard.api.ApiException
import com.queueguard.api.ErrorCode
import com.queueguard.api.writeApiError
import com.queueguard.service.QueueRegistry
import jakarta.servlet.FilterChain
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.http.HttpMethod
import org.springframework.security.web.servlet.util.matcher.PathPatternRequestMatcher
import org.springframework.web.filter.OncePerRequestFilter
import tools.jackson.databind.ObjectMapper

class ServiceAuthenticationFilter(
    private val registry: QueueRegistry,
    private val mapper: ObjectMapper,
) : OncePerRequestFilter() {
    private val healthPath = PathPatternRequestMatcher.withDefaults().matcher(HttpMethod.GET, "/actuator/health")
    private val queuePath = PathPatternRequestMatcher.withDefaults().matcher("/v1/queues/{queueId}/**")

    override fun doFilterInternal(request: HttpServletRequest, response: HttpServletResponse, chain: FilterChain) {
        if (healthPath.matches(request)) {
            chain.doFilter(request, response)
            return
        }
        val parts = request.getHeaders("Authorization").toList().singleOrNull()?.split(' ')
        val token = parts?.takeIf { it.size == 2 && it[0].equals("Bearer", true) }?.get(1)
        val principal = token?.takeIf { it.matches(Regex("[A-Za-z0-9_-]{32,256}")) }?.let(registry::authenticate)
        if (principal == null) {
            writeApiError(response, mapper, ErrorCode.UNAUTHORIZED)
            return
        }
        val queueMatch = queuePath.matcher(request)
        if (queueMatch.isMatch) {
            val queueId = queueMatch.variables.getValue("queueId")
            try {
                registry.policy(principal.clientId, queueId)
            } catch (e: ApiException) {
                writeApiError(response, mapper, e.code)
                return
            }
        }
        val context = SecurityContextHolder.createEmptyContext()
        context.authentication = UsernamePasswordAuthenticationToken.authenticated(principal, null, emptyList())
        SecurityContextHolder.setContext(context)
        response.setHeader("Cache-Control", "no-store")
        chain.doFilter(request, response)
    }
}
