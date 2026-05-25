package com.weatherflow.consumer

import com.weatherflow.config.Topics
import com.weatherflow.kafka.createConsumer
import com.weatherflow.model.WeatherPacket
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import kotlinx.coroutines.flow.MutableSharedFlow
import java.time.Duration

class WeatherConsumer(private val flow: MutableSharedFlow<WeatherPacket>) {

    private val mapper = jacksonObjectMapper()
    private val consumer = createConsumer("weather-consumer-group")

    fun start() {
        consumer.subscribe(listOf(Topics.CLEAN_WEATHER))

        while (true) {
            val records = consumer.poll(Duration.ofMillis(1000))

            for (record in records) {
                try {
                    val packet = mapper.readValue<WeatherPacket>(record.value())
                    kotlinx.coroutines.runBlocking {
                        flow.emit(packet)
                    }
                } catch (e: Exception) {
                    println("[WeatherConsumer] Failed to deserialize record: ${e.message}")
                }
            }
        }
    }
}