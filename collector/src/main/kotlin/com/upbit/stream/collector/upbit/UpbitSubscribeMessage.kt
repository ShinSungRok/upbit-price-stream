package com.upbit.stream.collector.upbit

import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import java.util.UUID

/** Builds the subscribe request Upbit expects as the first text frame after connecting. */
object UpbitSubscribeMessage {

    fun build(markets: List<String>): String {
        val codes = buildJsonArray { markets.forEach { add(JsonPrimitive(it)) } }
        val payload = buildJsonArray {
            add(buildJsonObject { put("ticket", "upbit-price-stream-${UUID.randomUUID()}") })
            add(
                buildJsonObject {
                    put("type", "ticker")
                    put("codes", codes)
                },
            )
            add(
                buildJsonObject {
                    put("type", "trade")
                    put("codes", codes)
                },
            )
            add(buildJsonObject { put("format", "DEFAULT") })
        }
        return payload.toString()
    }
}
