package com.upbit.stream.common.event

import kotlinx.serialization.Serializable

/** OHLCV candle produced by stream-processor's windowed aggregation over [TickerEvent]s. */
@Serializable
data class CandleEvent(
    val market: String,
    val windowStartEpochMillis: Long,
    val windowEndEpochMillis: Long,
    val open: Double,
    val high: Double,
    val low: Double,
    val close: Double,
    val volume: Double,
)
