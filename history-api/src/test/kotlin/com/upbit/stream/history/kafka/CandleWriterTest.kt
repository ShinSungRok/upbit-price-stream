package com.upbit.stream.history.kafka

import io.questdb.client.Sender
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.mockito.Mockito.verify
import java.time.temporal.ChronoUnit

class CandleWriterTest {

    private val sender: Sender = Mockito.mock(Sender::class.java, Mockito.RETURNS_SELF)
    private val writer = CandleWriter(sender)

    @Test
    fun `writes a valid candle to questdb via the fluent ILP builder`() {
        val payload = """
            {"market":"KRW-BTC","windowStartEpochMillis":1000,"windowEndEpochMillis":61000,
             "open":100.0,"high":110.0,"low":90.0,"close":105.0,"volume":5.5}
        """.trimIndent()

        writer.onCandle(payload)

        verify(sender).table("candles")
        verify(sender).symbol("market", "KRW-BTC")
        verify(sender).doubleColumn("open", 100.0)
        verify(sender).doubleColumn("close", 105.0)
        verify(sender).at(1000L, ChronoUnit.MILLIS)
    }

    @Test
    fun `skips malformed payloads without throwing`() {
        assertDoesNotThrow { writer.onCandle("not valid json") }
        Mockito.verifyNoInteractions(sender)
    }
}
