package com.upbit.stream.processor.serde

import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer

class KotlinxJsonSerde<T : Any>(
    private val serializer: KSerializer<T>,
    private val json: Json = Json { ignoreUnknownKeys = true },
) : Serde<T> {

    override fun serializer(): Serializer<T> =
        Serializer { _, data -> json.encodeToString(serializer, data).toByteArray(Charsets.UTF_8) }

    override fun deserializer(): Deserializer<T> =
        Deserializer { _, data -> json.decodeFromString(serializer, String(data, Charsets.UTF_8)) }
}
