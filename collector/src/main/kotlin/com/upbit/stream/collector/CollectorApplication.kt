package com.upbit.stream.collector

import com.upbit.stream.collector.config.UpbitWebSocketProperties
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication

@SpringBootApplication
@EnableConfigurationProperties(UpbitWebSocketProperties::class)
class CollectorApplication

fun main(args: Array<String>) {
    runApplication<CollectorApplication>(*args)
}
