package com.upbit.stream.processor.config

import com.upbit.stream.processor.topology.CandleTopology
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.KStream
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafkaStreams

@Configuration
@EnableKafkaStreams
class CandleAggregationTopologyConfig {

    @Bean
    fun candleAggregationStream(streamsBuilder: StreamsBuilder): KStream<String, String> =
        CandleTopology.build(streamsBuilder)
}
