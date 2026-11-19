package com.queueguard.config

import com.queueguard.api.ErrorCode
import com.queueguard.api.writeApiError
import com.queueguard.service.QueueRegistry
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.HttpMethod
import org.springframework.security.config.annotation.web.builders.HttpSecurity
import org.springframework.security.config.http.SessionCreationPolicy
import org.springframework.security.provisioning.InMemoryUserDetailsManager
import org.springframework.security.web.SecurityFilterChain
import org.springframework.security.web.authentication.AnonymousAuthenticationFilter
import org.springframework.security.config.annotation.web.configuration.WebSecurityCustomizer
import tools.jackson.databind.ObjectMapper

@Configuration(proxyBeanMethods = false)
class SecurityConfig {
    @Bean
    fun securityFilterChain(http: HttpSecurity, registry: QueueRegistry, mapper: ObjectMapper): SecurityFilterChain {
        // Cookie·세션을 사용하지 않는 서버 간 Bearer 인증에 한정한다.
        http.csrf { it.disable() }
        http.logout { it.disable() }
        http.authorizeHttpRequests {
            it.requestMatchers(HttpMethod.GET, "/actuator/health").permitAll()
                .requestMatchers("/v1/queues/**").authenticated()
                .anyRequest().denyAll()
        }
        http.exceptionHandling {
            it.authenticationEntryPoint { _, response, _ -> writeApiError(response, mapper, ErrorCode.UNAUTHORIZED) }
            it.accessDeniedHandler { _, response, _ -> writeApiError(response, mapper, ErrorCode.FORBIDDEN) }
        }
        http.sessionManagement { it.sessionCreationPolicy(SessionCreationPolicy.STATELESS) }
        http.requestCache { it.disable() }
        http.addFilterBefore(ServiceAuthenticationFilter(registry, mapper), AnonymousAuthenticationFilter::class.java)
        return http.build()
    }

    @Bean
    fun firewallErrors(mapper: ObjectMapper) = WebSecurityCustomizer { web ->
        web.requestRejectedHandler { _, response, _ -> writeApiError(response, mapper, ErrorCode.INVALID_REQUEST) }
    }

    @Bean
    fun userDetailsService() = InMemoryUserDetailsManager()
}
