package com.upbit.stream.api.stream

import com.upbit.stream.common.avro.CandleEventAvroMapper
import com.upbit.stream.common.event.CandleEvent
import com.upbit.stream.common.kafka.KafkaTopics
import kotlinx.serialization.json.Json
import org.apache.avro.generic.GenericRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

/**
 * Consumes candle.1m (Avro) separately from [MarketDataBroadcaster]'s ticker listener (see that
 * class for why) and re-publishes each candle as JSON into the same WebSocket sink — the wire
 * format switch to Avro on this internal topic is invisible to `/ws/stream` clients.
 */
@Component
class CandleBroadcastListener(private val broadcaster: MarketDataBroadcaster) {
    private val json = Json { ignoreUnknownKeys = true }

    @KafkaListener(
        topics = [KafkaTopics.CANDLE_1M],
        groupId = "api-server",
        containerFactory = "avroKafkaListenerContainerFactory",
    )
    fun onCandle(record: GenericRecord) {
        val candle: CandleEvent = CandleEventAvroMapper.fromGenericRecord(record)
        broadcaster.publish(json.encodeToString(CandleEvent.serializer(), candle))
    }
}
