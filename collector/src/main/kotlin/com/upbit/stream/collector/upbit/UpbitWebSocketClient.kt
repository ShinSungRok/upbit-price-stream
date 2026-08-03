package com.upbit.stream.collector.upbit

import com.upbit.stream.collector.config.UpbitWebSocketProperties
import com.upbit.stream.common.event.TickerEvent
import com.upbit.stream.common.event.TradeEvent
import com.upbit.stream.common.kafka.KafkaTopics
import jakarta.annotation.PreDestroy
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.slf4j.LoggerFactory
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import java.io.ByteArrayOutputStream
import java.net.URI
import java.net.http.HttpClient
import java.net.http.WebSocket
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.concurrent.CompletionStage
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * Connects to Upbit's public WebSocket feed and republishes every ticker/trade frame onto Kafka.
 *
 * Upbit sends frames as *binary* (not text), and periodically closes long-lived idle
 * connections server-side, so this listener both reassembles fragmented binary frames and
 * reconnects automatically on any close/error.
 */
@Component
class UpbitWebSocketClient(
    private val properties: UpbitWebSocketProperties,
    private val kafkaTemplate: KafkaTemplate<String, String>,
) : ApplicationRunner, WebSocket.Listener {

    private val log = LoggerFactory.getLogger(UpbitWebSocketClient::class.java)

    // The JDK's default WebSocket I/O threads don't inherit the Spring Boot fat jar's
    // launcher classloader, which breaks lazy classloading (e.g. Kafka's ConfigDef.parseType)
    // the first time it happens off the main thread. Pin the app classloader explicitly.
    private val httpClient = HttpClient.newBuilder()
        .executor(
            Executors.newCachedThreadPool { runnable ->
                Thread(runnable, "upbit-ws-io").apply {
                    isDaemon = true
                    contextClassLoader = UpbitWebSocketClient::class.java.classLoader
                }
            },
        )
        .build()
    private val json = Json { ignoreUnknownKeys = true }
    private val reconnectExecutor = Executors.newSingleThreadScheduledExecutor { runnable ->
        Thread(runnable, "upbit-ws-reconnect").apply { isDaemon = true }
    }
    private val messageBuffer = ByteArrayOutputStream()

    @Volatile
    private var webSocket: WebSocket? = null

    @Volatile
    private var shuttingDown = false

    override fun run(args: ApplicationArguments) {
        connect()
    }

    private fun connect() {
        log.info("Connecting to Upbit WebSocket at {}", properties.uri)
        httpClient.newWebSocketBuilder()
            .buildAsync(URI.create(properties.uri), this)
            .whenComplete { ws, error ->
                if (error != null) {
                    log.warn("Failed to connect to Upbit WebSocket: {}", error.message)
                    scheduleReconnect()
                } else {
                    webSocket = ws
                }
            }
    }

    private fun scheduleReconnect() {
        if (shuttingDown) return
        log.info("Reconnecting to Upbit WebSocket in {}s", properties.reconnectDelaySeconds)
        reconnectExecutor.schedule({ connect() }, properties.reconnectDelaySeconds, TimeUnit.SECONDS)
    }

    override fun onOpen(webSocket: WebSocket) {
        log.info("Upbit WebSocket connected, subscribing to markets: {}", properties.markets)
        webSocket.sendText(UpbitSubscribeMessage.build(properties.markets), true)
        webSocket.request(Long.MAX_VALUE)
    }

    override fun onBinary(webSocket: WebSocket, data: ByteBuffer, last: Boolean): CompletionStage<*>? {
        val chunk = ByteArray(data.remaining())
        data.get(chunk)
        messageBuffer.write(chunk)
        if (last) {
            val payload = messageBuffer.toString(StandardCharsets.UTF_8)
            messageBuffer.reset()
            handleMessage(payload)
        }
        return null
    }

    private fun handleMessage(payload: String) {
        val type = runCatching {
            json.parseToJsonElement(payload).jsonObject["type"]?.jsonPrimitive?.content
        }.getOrNull()

        try {
            when (type) {
                "ticker" -> {
                    val ticker = json.decodeFromString(TickerEvent.serializer(), payload)
                    kafkaTemplate.send(KafkaTopics.UPBIT_TICKER_RAW, ticker.market, payload)
                }
                "trade" -> {
                    val trade = json.decodeFromString(TradeEvent.serializer(), payload)
                    kafkaTemplate.send(KafkaTopics.UPBIT_TRADE_RAW, trade.market, payload)
                }
                else -> log.debug("Ignoring unrecognized Upbit message type: {}", type)
            }
        } catch (ex: Exception) {
            log.warn("Failed to parse Upbit message, dropping frame: {}", ex.message)
        }
    }

    override fun onError(webSocket: WebSocket, error: Throwable) {
        log.warn("Upbit WebSocket error", error)
        scheduleReconnect()
    }

    override fun onClose(webSocket: WebSocket, statusCode: Int, reason: String): CompletionStage<*>? {
        log.info("Upbit WebSocket closed: {} {}", statusCode, reason)
        scheduleReconnect()
        return null
    }

    @PreDestroy
    fun shutdown() {
        shuttingDown = true
        webSocket?.sendClose(WebSocket.NORMAL_CLOSURE, "shutdown")
        reconnectExecutor.shutdownNow()
    }
}
