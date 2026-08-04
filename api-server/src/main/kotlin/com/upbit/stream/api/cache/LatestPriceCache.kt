package com.upbit.stream.api.cache

import com.upbit.stream.common.event.TickerEvent
import com.upbit.stream.common.kafka.KafkaTopics
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import org.springframework.data.redis.core.ReactiveStringRedisTemplate
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import java.time.Duration

private val CACHE_TTL: Duration = Duration.ofHours(24)

/**
 * Caches the latest raw ticker per market in Valkey so `/api/latest/{market}` can answer
 * without a WebSocket connection. Runs in its own consumer group ("api-server-cache") rather
 * than joining MarketDataBroadcaster's "api-server" group — sharing a group would split
 * partitions between the two listeners, so each would only see some of the traffic.
 */
@Component
class LatestPriceCache(private val redis: ReactiveStringRedisTemplate) {
    private val log = LoggerFactory.getLogger(LatestPriceCache::class.java)
    private val json = Json { ignoreUnknownKeys = true }

    @KafkaListener(topics = [KafkaTopics.UPBIT_TICKER_RAW], groupId = "api-server-cache")
    fun onTicker(payload: String) {
        val ticker = runCatching { json.decodeFromString(TickerEvent.serializer(), payload) }
            .getOrElse {
                log.warn("Failed to parse ticker payload, skipping cache write: {}", it.message)
                return
            }

        redis.opsForValue().set("latest:${ticker.market}", payload, CACHE_TTL).block()
    }
}
