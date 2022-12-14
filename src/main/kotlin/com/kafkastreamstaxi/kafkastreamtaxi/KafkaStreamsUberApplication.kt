package com.kafkastreamstaxi.kafkastreamtaxi

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication


@SpringBootApplication
class KafkaStreamsUberApplication

fun main(args: Array<String>) {
    runApplication<KafkaStreamsUberApplication>(*args)
}