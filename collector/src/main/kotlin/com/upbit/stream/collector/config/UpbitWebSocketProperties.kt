package com.upbit.stream.collector.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "upbit.websocket")
data class UpbitWebSocketProperties(
    val uri: String,
    val markets: List<String>,
    val reconnectDelaySeconds: Long,
)
