package com.upbit.stream.processor.config

import com.upbit.stream.processor.topology.CandleTopology
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KStream
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafkaStreams
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration
import org.springframework.kafka.config.KafkaStreamsConfiguration

@Configuration
@EnableKafkaStreams
class CandleAggregationTopologyConfig(
    @param:Value("\${spring.kafka.bootstrap-servers}") private val bootstrapServers: String,
    @param:Value("\${spring.kafka.streams.application-id}") private val applicationId: String,
) {

    @Bean(name = [KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME])
    fun defaultKafkaStreamsConfig(): KafkaStreamsConfiguration {
        val props = mapOf(
            StreamsConfig.APPLICATION_ID_CONFIG to applicationId,
            StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
        )
        return KafkaStreamsConfiguration(props)
    }

    @Bean
    fun candleAggregationStream(streamsBuilder: StreamsBuilder): KStream<String, String> =
        CandleTopology.build(streamsBuilder)
}
