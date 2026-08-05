package com.upbit.stream.history.kafka

import com.upbit.stream.common.avro.CandleEventAvroMapper
import com.upbit.stream.common.kafka.KafkaTopics
import io.questdb.client.Sender
import org.apache.avro.generic.GenericRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import java.time.temporal.ChronoUnit

@Component
class CandleWriter(private val sender: Sender) {

    @KafkaListener(topics = [KafkaTopics.CANDLE_1M], groupId = "history-api")
    fun onCandle(record: GenericRecord) {
        val candle = CandleEventAvroMapper.fromGenericRecord(record)

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
