package com.upbit.stream.api.stream

import com.upbit.stream.common.kafka.KafkaTopics
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Sinks

/**
 * Bridges the live Kafka streams into an in-process multicast [Sinks.Many] that any number of
 * connected WebSocket clients can subscribe to. Only handles the raw ticker topic itself (plain
 * String/JSON, default consumer factory); [com.upbit.stream.api.stream.CandleBroadcastListener]
 * publishes candle.1m (Avro, its own listener container factory) into the same sink via [publish]
 * — Spring Kafka's ConsumerFactory only supports one value deserializer per factory, so the two
 * topics can't share a single @KafkaListener anymore now that candle.1m is Avro-encoded.
 *
 * Uses a regular (blocking) spring-kafka [KafkaListener] rather than a reactive Kafka client:
 * reactor-kafka was discontinued in May 2025, and Spring's own reactive Kafka template is being
 * deprecated alongside it, so a `@KafkaListener` pushing into a [Sinks.Many] is the current
 * recommended way to bridge Kafka into a reactive/WebFlux pipeline.
 */
@Service
class MarketDataBroadcaster {
    private val log = LoggerFactory.getLogger(MarketDataBroadcaster::class.java)
    private val sink: Sinks.Many<String> = Sinks.many().multicast().onBackpressureBuffer()

    fun stream(): Flux<String> = sink.asFlux()

    fun publish(payload: String) {
        val result = sink.tryEmitNext(payload)
        if (result.isFailure) {
            log.warn("Failed to emit market data to WebSocket sink: {}", result)
        }
    }

    @KafkaListener(topics = [KafkaTopics.UPBIT_TICKER_RAW], groupId = "api-server")
    fun onTicker(payload: String) = publish(payload)
}
