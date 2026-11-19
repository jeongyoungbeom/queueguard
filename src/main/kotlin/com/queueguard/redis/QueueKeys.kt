package com.queueguard.redis

import com.queueguard.config.IDENTIFIER_PATTERN

class QueueKeys(clientId: String, queueId: String) {
    init {
        require(clientId.matches(Regex(IDENTIFIER_PATTERN)) && queueId.matches(Regex(IDENTIFIER_PATTERN))) {
            "Invalid queue namespace"
        }
    }

    private val prefix = "{qg:$clientId:$queueId}"
    val config = "$prefix:config"
    val sequence = "$prefix:sequence"
    val waiting = "$prefix:waiting"
    val waitingExpirations = "$prefix:waiting-expirations"
    val active = "$prefix:active"
    val subjects = "$prefix:subjects"
    val entries = "$prefix:entries"
    val admissions = "$prefix:admissions"
    val rate = "$prefix:rate"
    val gc = "$prefix:gc"
    val all = listOf(config, sequence, waiting, waitingExpirations, active, subjects, entries, admissions, rate, gc)
}
