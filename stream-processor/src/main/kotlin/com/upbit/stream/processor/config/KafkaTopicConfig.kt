package com.upbit.stream.processor.config

import com.upbit.stream.common.kafka.KafkaTopics
import org.apache.kafka.clients.admin.NewTopic
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder

/**
 * Kafka Streams fails fast at startup with [org.apache.kafka.streams.errors.MissingSourceTopicException]
 * if its source topic doesn't already exist yet (unlike a plain consumer, which just retries).
 * Declaring both the source and sink topics here makes this service resilient to start-up ordering.
 */
@Configuration
class KafkaTopicConfig {

    @Bean
    fun upbitTickerRawTopic(): NewTopic =
        TopicBuilder.name(KafkaTopics.UPBIT_TICKER_RAW).partitions(3).replicas(1).build()

    @Bean
    fun candle1mTopic(): NewTopic =
        TopicBuilder.name(KafkaTopics.CANDLE_1M).partitions(3).replicas(1).build()
}
