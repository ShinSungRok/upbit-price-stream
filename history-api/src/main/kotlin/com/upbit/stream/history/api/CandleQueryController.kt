package com.upbit.stream.history.api

import com.upbit.stream.common.event.CandleEvent
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.sql.Timestamp

@RestController
class CandleQueryController(private val jdbcTemplate: JdbcTemplate) {

    @GetMapping("/api/candles")
    fun getCandles(
        @RequestParam market: String,
        @RequestParam from: Long,
        @RequestParam to: Long,
    ): List<CandleEvent> =
        jdbcTemplate.query(
            """
            SELECT window_start, window_end, market, open, high, low, close, volume
            FROM candles
            WHERE market = ? AND window_start BETWEEN ? AND ?
            ORDER BY window_start
            """.trimIndent(),
            { rs, _ ->
                CandleEvent(
                    market = rs.getString("market"),
                    windowStartEpochMillis = rs.getTimestamp("window_start").time,
                    windowEndEpochMillis = rs.getTimestamp("window_end").time,
                    open = rs.getDouble("open"),
                    high = rs.getDouble("high"),
                    low = rs.getDouble("low"),
                    close = rs.getDouble("close"),
                    volume = rs.getDouble("volume"),
                )
            },
            market,
            Timestamp(from),
            Timestamp(to),
        )
}
