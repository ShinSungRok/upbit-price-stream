package com.upbit.stream.api.ws

import com.upbit.stream.api.stream.MarketDataBroadcaster
import org.springframework.stereotype.Component
import org.springframework.web.reactive.socket.WebSocketHandler
import org.springframework.web.reactive.socket.WebSocketSession
import reactor.core.publisher.Mono

@Component
class MarketDataWebSocketHandler(
    private val broadcaster: MarketDataBroadcaster,
) : WebSocketHandler {

    override fun handle(session: WebSocketSession): Mono<Void> =
        session.send(broadcaster.stream().map(session::textMessage))
}
