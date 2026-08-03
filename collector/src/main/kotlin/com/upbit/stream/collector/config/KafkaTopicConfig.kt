package com.upbit.stream.collector.config

import com.upbit.stream.common.kafka.KafkaTopics
import org.apache.kafka.clients.admin.NewTopic
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder

@Configuration
class KafkaTopicConfig {

    @Bean
    fun upbitTickerRawTopic(): NewTopic =
        TopicBuilder.name(KafkaTopics.UPBIT_TICKER_RAW).partitions(3).replicas(1).build()

    @Bean
    fun upbitTradeRawTopic(): NewTopic =
        TopicBuilder.name(KafkaTopics.UPBIT_TRADE_RAW).partitions(3).replicas(1).build()
}
