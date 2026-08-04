package com.upbit.stream.history.kafka

import com.upbit.stream.common.event.CandleEvent
import com.upbit.stream.common.kafka.KafkaTopics
import io.questdb.client.Sender
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import java.time.temporal.ChronoUnit

@Component
class CandleWriter(private val sender: Sender) {
    private val log = LoggerFactory.getLogger(CandleWriter::class.java)
    private val json = Json { ignoreUnknownKeys = true }

    @KafkaListener(topics = [KafkaTopics.CANDLE_1M], groupId = "history-api")
    fun onCandle(payload: String) {
        val candle = runCatching { json.decodeFromString(CandleEvent.serializer(), payload) }
            .getOrElse {
                log.warn("Failed to parse candle payload, skipping: {}", it.message)
                return
            }

        sender.table("candles")
            .symbol("market", candle.market)
            .timestampColumn("window_end", candle.windowEndEpochMillis, ChronoUnit.MILLIS)
            .doubleColumn("open", candle.open)
            .doubleColumn("high", candle.high)
            .doubleColumn("low", candle.low)
            .doubleColumn("close", candle.close)
            .doubleColumn("volume", candle.volume)
            .at(candle.windowStartEpochMillis, ChronoUnit.MILLIS)
    }
}
