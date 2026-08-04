package com.upbit.stream.api.api

import org.springframework.data.redis.core.ReactiveStringRedisTemplate
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Mono

@RestController
class LatestPriceController(private val redis: ReactiveStringRedisTemplate) {

    @GetMapping("/api/latest/{market}")
    fun latest(@PathVariable market: String): Mono<ResponseEntity<String>> =
        redis.opsForValue().get("latest:$market")
            .map { ResponseEntity.ok(it) }
            .defaultIfEmpty(ResponseEntity.notFound().build())
}
