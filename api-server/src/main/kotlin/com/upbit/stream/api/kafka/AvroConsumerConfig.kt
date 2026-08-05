package com.upbit.stream.api.kafka

import io.apicurio.registry.resolver.config.SchemaResolverConfig
import io.apicurio.registry.serde.avro.AvroKafkaDeserializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory

/**
 * A second listener container factory alongside Spring Boot's default (String-valued) one, for
 * candle.1m specifically — Spring Kafka's ConsumerFactory only supports one value deserializer,
 * so a topic encoded differently (Avro, via Apicurio's registry-backed deserializer) needs its
 * own factory rather than reusing the app-wide default.
 */
@Configuration
class AvroConsumerConfig(
    @param:Value("\${spring.kafka.bootstrap-servers}") private val bootstrapServers: String,
    @param:Value("\${apicurio.registry.url}") private val registryUrl: String,
) {

    @Bean
    fun avroKafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, GenericRecord> {
        val consumerFactory = DefaultKafkaConsumerFactory(
            mapOf(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG to "api-server",
                SchemaResolverConfig.REGISTRY_URL to registryUrl,
            ),
            StringDeserializer(),
            AvroKafkaDeserializer<GenericRecord>(),
        )
        return ConcurrentKafkaListenerContainerFactory<String, GenericRecord>().apply {
            setConsumerFactory(consumerFactory)
        }
    }
}
