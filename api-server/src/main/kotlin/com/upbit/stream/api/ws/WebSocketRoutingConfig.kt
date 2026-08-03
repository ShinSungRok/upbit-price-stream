package com.upbit.stream.api.ws

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.reactive.HandlerMapping
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter

@Configuration
class WebSocketRoutingConfig {

    @Bean
    fun webSocketMapping(handler: MarketDataWebSocketHandler): HandlerMapping =
        SimpleUrlHandlerMapping(mapOf("/ws/stream" to handler), -1)

    @Bean
    fun webSocketHandlerAdapter(): WebSocketHandlerAdapter = WebSocketHandlerAdapter()
}
