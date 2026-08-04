package com.upbit.stream.history.questdb

import io.questdb.client.Sender
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class QuestDbSenderConfig(
    @param:Value("\${questdb.ilp-address}") private val ilpAddress: String,
) {

    // QuestDB's Sender is not thread-safe; sharing one bean is only safe because
    // CandleWriter's @KafkaListener runs on a single consumer thread by default.
    @Bean
    fun questDbSender(): Sender =
        Sender.builder(Sender.Transport.HTTP)
            .address(ilpAddress)
            .autoFlushRows(1)
            .build()
}
