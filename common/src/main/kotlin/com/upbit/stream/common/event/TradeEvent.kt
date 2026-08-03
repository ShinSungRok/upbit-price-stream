package com.upbit.stream.common.event

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Mirrors Upbit's WebSocket trade payload (DEFAULT format field names). */
@Serializable
data class TradeEvent(
    @SerialName("type") val type: String,
    @SerialName("code") val market: String,
    @SerialName("trade_price") val tradePrice: Double,
    @SerialName("trade_volume") val tradeVolume: Double,
    @SerialName("ask_bid") val askBid: String,
    @SerialName("prev_closing_price") val prevClosingPrice: Double,
    @SerialName("change") val change: String,
    @SerialName("change_price") val changePrice: Double,
    @SerialName("sequential_id") val sequentialId: Long,
    @SerialName("trade_timestamp") val tradeTimestamp: Long,
    @SerialName("timestamp") val timestamp: Long,
)
