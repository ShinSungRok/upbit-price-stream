package com.upbit.stream.processor.config

import com.upbit.stream.processor.serde.AvroRegistrySerde
import com.upbit.stream.processor.topology.CandleTopology
import io.apicurio.registry.resolver.config.SchemaResolverConfig
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.KStream
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafkaStreams

@Configuration
@EnableKafkaStreams
class CandleAggregationTopologyConfig(
    @param:Value("\${apicurio.registry.url}") private val registryUrl: String,
) {

    @Bean
    fun candleAggregationStream(streamsBuilder: StreamsBuilder): KStream<String, GenericRecord> {
        val candleSerde = AvroRegistrySerde(
            mapOf(
                SchemaResolverConfig.REGISTRY_URL to registryUrl,
                SchemaResolverConfig.AUTO_REGISTER_ARTIFACT to true,
            ),
        )
        return CandleTopology.build(streamsBuilder, candleSerde)
    }
}
