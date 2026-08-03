package com.upbit.stream.processor.candle

import com.upbit.stream.common.event.CandleEvent
import com.upbit.stream.common.event.TickerEvent
import kotlinx.serialization.Serializable

/** Running OHLCV state for one window, refined with each new tick via [accumulate]. */
@Serializable
data class CandleAccumulator(
    val open: Double? = null,
    val high: Double = Double.NEGATIVE_INFINITY,
    val low: Double = Double.POSITIVE_INFINITY,
    val close: Double? = null,
    val volume: Double = 0.0,
) {
    fun accumulate(ticker: TickerEvent): CandleAccumulator = copy(
        open = open ?: ticker.tradePrice,
        high = maxOf(high, ticker.tradePrice),
        low = minOf(low, ticker.tradePrice),
        close = ticker.tradePrice,
        volume = volume + ticker.tradeVolume,
    )

    fun toCandleEvent(market: String, windowStart: Long, windowEnd: Long): CandleEvent = CandleEvent(
        market = market,
        windowStartEpochMillis = windowStart,
        windowEndEpochMillis = windowEnd,
        open = open ?: 0.0,
        high = if (high.isFinite()) high else 0.0,
        low = if (low.isFinite()) low else 0.0,
        close = close ?: 0.0,
        volume = volume,
    )
}
