package com.upbit.stream.history.questdb

import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Component

/**
 * DEDUP UPSERT KEYS(window_start, market) collapses the repeated partial-candle
 * updates stream-processor emits for the same window (it emits on every tick,
 * not just when the window closes) down to one row per (market, minute) —
 * each new write for an already-seen key replaces the previous one.
 */
@Component
class QuestDbSchemaInitializer(private val jdbcTemplate: JdbcTemplate) : ApplicationRunner {

    override fun run(args: ApplicationArguments) {
        jdbcTemplate.execute(
            """
            CREATE TABLE IF NOT EXISTS candles (
                window_start TIMESTAMP,
                window_end TIMESTAMP,
                market SYMBOL,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE
            ) TIMESTAMP(window_start) PARTITION BY DAY WAL
            DEDUP UPSERT KEYS(window_start, market)
            """.trimIndent(),
        )
    }
}
