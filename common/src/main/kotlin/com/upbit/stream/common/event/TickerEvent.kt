package com.upbit.stream.common.event

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Mirrors Upbit's WebSocket ticker payload (SIMPLE format uses abbreviated keys;
 * this DTO expects the DEFAULT format field names as sent by the collector's subscribe request).
 */
@Serializable
data class TickerEvent(
    @SerialName("type") val type: String,
    @SerialName("code") val market: String,
    @SerialName("opening_price") val openingPrice: Double,
    @SerialName("high_price") val highPrice: Double,
    @SerialName("low_price") val lowPrice: Double,
    @SerialName("trade_price") val tradePrice: Double,
    @SerialName("prev_closing_price") val prevClosingPrice: Double,
    @SerialName("change") val change: String,
    @SerialName("change_price") val changePrice: Double,
    @SerialName("signed_change_price") val signedChangePrice: Double,
    @SerialName("change_rate") val changeRate: Double,
    @SerialName("signed_change_rate") val signedChangeRate: Double,
    @SerialName("trade_volume") val tradeVolume: Double,
    @SerialName("acc_trade_volume") val accTradeVolume: Double,
    @SerialName("acc_trade_price") val accTradePrice: Double,
    @SerialName("trade_timestamp") val tradeTimestamp: Long,
    @SerialName("timestamp") val timestamp: Long,
)
