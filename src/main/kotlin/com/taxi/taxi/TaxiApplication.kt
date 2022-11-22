package com.taxi.taxi

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class TaxiApplication

fun main(args: Array<String>) {
	runApplication<TaxiApplication>(*args)
}
