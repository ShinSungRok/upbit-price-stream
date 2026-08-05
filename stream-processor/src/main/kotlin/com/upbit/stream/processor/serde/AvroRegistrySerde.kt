package com.upbit.stream.processor.serde

import io.apicurio.registry.serde.avro.AvroKafkaDeserializer
import io.apicurio.registry.serde.avro.AvroKafkaSerializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer

/** Wraps Apicurio's registry-backed Avro Kafka serializer/deserializer as a Kafka Streams [Serde]. */
class AvroRegistrySerde(config: Map<String, Any>) : Serde<GenericRecord> {
    private val avroSerializer = AvroKafkaSerializer<GenericRecord>().apply { configure(config, false) }
    private val avroDeserializer = AvroKafkaDeserializer<GenericRecord>().apply { configure(config, false) }

    override fun serializer(): Serializer<GenericRecord> = avroSerializer

    override fun deserializer(): Deserializer<GenericRecord> = avroDeserializer
}
