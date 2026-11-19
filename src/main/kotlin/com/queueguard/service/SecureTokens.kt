package com.queueguard.service

import java.security.SecureRandom
import java.util.Base64
import java.util.UUID
import org.springframework.stereotype.Component

@Component
class SecureTokens {
    private val random = SecureRandom()

    fun newAdmissionToken(): String = Base64.getUrlEncoder().withoutPadding()
        .encodeToString(ByteArray(32).also(random::nextBytes))

    fun newId(): String = UUID.randomUUID().toString()
}
