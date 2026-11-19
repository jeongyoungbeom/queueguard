package com.queueguard.config

import org.springframework.boot.jackson.autoconfigure.JsonMapperBuilderCustomizer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import tools.jackson.databind.cfg.CoercionAction
import tools.jackson.databind.cfg.CoercionInputShape

@Configuration(proxyBeanMethods = false)
class JacksonConfig {
    @Bean
    fun strictStringInputs() = JsonMapperBuilderCustomizer { builder ->
        // 식별자 오입력을 다른 사용자의 문자열로 바꾸지 않는다.
        builder.withCoercionConfig(String::class.java) {
            it.setCoercion(CoercionInputShape.Boolean, CoercionAction.Fail)
            it.setCoercion(CoercionInputShape.Integer, CoercionAction.Fail)
            it.setCoercion(CoercionInputShape.Float, CoercionAction.Fail)
        }
    }
}
