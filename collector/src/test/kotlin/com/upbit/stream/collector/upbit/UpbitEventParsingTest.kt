package com.upbit.stream.collector.upbit

import com.upbit.stream.common.event.TickerEvent
import com.upbit.stream.common.event.TradeEvent
import kotlinx.serialization.json.Json
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class UpbitEventParsingTest {

    private val json = Json { ignoreUnknownKeys = true }

    private val sampleTicker = """
        {
          "type": "ticker",
          "code": "KRW-BTC",
          "opening_price": 83000000.0,
          "high_price": 84500000.0,
          "low_price": 82100000.0,
          "trade_price": 83920000.0,
          "prev_closing_price": 83010000.0,
          "change": "RISE",
          "change_price": 910000.0,
          "signed_change_price": 910000.0,
          "change_rate": 0.01096,
          "signed_change_rate": 0.01096,
          "trade_volume": 0.00231,
          "acc_trade_volume": 1523.442,
          "acc_trade_price": 126532345123.4,
          "trade_timestamp": 1753600000000,
          "timestamp": 1753600000010
        }
    """.trimIndent()

    private val sampleTrade = """
        {
          "type": "trade",
          "code": "KRW-ETH",
          "trade_price": 4820000.0,
          "trade_volume": 0.5123,
          "ask_bid": "BID",
          "prev_closing_price": 4790000.0,
          "change": "RISE",
          "change_price": 30000.0,
          "sequential_id": 1753600000000123,
          "trade_timestamp": 1753600000000,
          "timestamp": 1753600000010
        }
    """.trimIndent()

    @Test
    fun `decodes a ticker frame into TickerEvent with correct market and price`() {
        val ticker = json.decodeFromString(TickerEvent.serializer(), sampleTicker)

        assertThat(ticker.market).isEqualTo("KRW-BTC")
        assertThat(ticker.tradePrice).isEqualTo(83920000.0)
        assertThat(ticker.openingPrice).isEqualTo(83000000.0)
        assertThat(ticker.highPrice).isEqualTo(84500000.0)
        assertThat(ticker.lowPrice).isEqualTo(82100000.0)
    }

    @Test
    fun `decodes a trade frame into TradeEvent with correct market and side`() {
        val trade = json.decodeFromString(TradeEvent.serializer(), sampleTrade)

        assertThat(trade.market).isEqualTo("KRW-ETH")
        assertThat(trade.askBid).isEqualTo("BID")
        assertThat(trade.tradeVolume).isEqualTo(0.5123)
    }

    @Test
    fun `subscribe message includes all requested markets for both ticker and trade types`() {
        val markets = listOf("KRW-BTC", "KRW-ETH", "KRW-XRP")

        val message = UpbitSubscribeMessage.build(markets)
        val elements = json.parseToJsonElement(message)

        assertThat(message).contains("\"type\":\"ticker\"")
        assertThat(message).contains("\"type\":\"trade\"")
        markets.forEach { market ->
            assertThat(message).contains("\"$market\"")
        }
        assertThat(elements.toString()).contains("DEFAULT")
    }
}
