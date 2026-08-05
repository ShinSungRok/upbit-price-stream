package com.upbit.stream.common.avro

import com.upbit.stream.common.event.CandleEvent
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord

/**
 * Converts between [CandleEvent] and the Avro [GenericRecord] shape used on the wire for
 * candle.1m (see avro/candle-event.avsc). Shared here so stream-processor (producer) and
 * api-server/history-api (consumers) don't each reimplement the same field mapping.
 */
object CandleEventAvroMapper {

    val SCHEMA: Schema = Schema.Parser().parse(
        CandleEventAvroMapper::class.java.classLoader.getResourceAsStream("avro/candle-event.avsc")
            ?: error("avro/candle-event.avsc not found on classpath"),
    )

    fun toGenericRecord(candle: CandleEvent): GenericRecord =
        GenericData.Record(SCHEMA).apply {
            put("market", candle.market)
            put("windowStartEpochMillis", candle.windowStartEpochMillis)
            put("windowEndEpochMillis", candle.windowEndEpochMillis)
            put("open", candle.open)
            put("high", candle.high)
            put("low", candle.low)
            put("close", candle.close)
            put("volume", candle.volume)
        }

    fun fromGenericRecord(record: GenericRecord): CandleEvent = CandleEvent(
        market = record.get("market").toString(),
        windowStartEpochMillis = record.get("windowStartEpochMillis") as Long,
        windowEndEpochMillis = record.get("windowEndEpochMillis") as Long,
        open = record.get("open") as Double,
        high = record.get("high") as Double,
        low = record.get("low") as Double,
        close = record.get("close") as Double,
        volume = record.get("volume") as Double,
    )
}
