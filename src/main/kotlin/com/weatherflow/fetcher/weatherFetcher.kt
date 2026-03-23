package com.weatherflow.consumer

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.weatherflow.model.WeatherPacket
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.Properties

class WeatherConsumer(private val onPacket: (WeatherPacket) -> Unit) {

    private val mapper = jacksonObjectMapper()

    private val consumer = KafkaConsumer<String, String>(Properties().apply {
        put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        put(ConsumerConfig.GROUP_ID_CONFIG, "weather-consumer-group")
        put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    })

    fun start() {
        consumer.subscribe(listOf("clean-weather"))
        println("[WeatherConsumer] Listening on clean-weather...")
        try {
            while (true) {
                val records = consumer.poll(Duration.ofMillis(1000))
                for (record in records) {
                    try {
                        val packet = mapper.readValue<WeatherPacket>(record.value())
                        println("[WeatherConsumer] 📦 Received: ${packet.city} ${packet.temperature}°C")
                        onPacket(packet)
                    } catch (e: Exception) {
                        println("[WeatherConsumer] Failed to deserialize: ${e.message}")
                    }
                }
            }
        } finally {
            consumer.close()
        }
    }
}