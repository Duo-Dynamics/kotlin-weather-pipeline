package com.weatherflow.filter

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.weatherflow.model.WeatherPacket
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.time.Duration
import java.util.Properties

class FilterServer {

    private val mapper = jacksonObjectMapper()

    private val consumer = KafkaConsumer<String, String>(Properties().apply {
        put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        put(ConsumerConfig.GROUP_ID_CONFIG, "filter-server-group")
        put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    })

    private val producer = KafkaProducer<String, String>(Properties().apply {
        put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
    })

    fun start() {
        consumer.subscribe(listOf("verified-weather"))
        println("[FilterServer] Listening on verified-weather...")
        try {
            while (true) {
                val records = consumer.poll(Duration.ofMillis(1000))
                for (record in records) {
                    processRecord(record.value())
                }
            }
        } finally {
            consumer.close()
            producer.close()
        }
    }

    private fun processRecord(json: String) {
        val packet = try {
            mapper.readValue<WeatherPacket>(json)
        } catch (e: Exception) {
            println("[FilterServer] Failed to deserialize: ${e.message}")
            return
        }

        if (isValid(packet)) {
            println("[FilterServer] ✅ Valid: ${packet.city}")
            producer.send(ProducerRecord("clean-weather", packet.city, json))
        } else {
            println("[FilterServer] ❌ Rejected: ${packet.city}")
            producer.send(ProducerRecord("filter-rejected", packet.city, json))
        }
    }

    private fun isValid(packet: WeatherPacket): Boolean {
        return packet.temperature in -100.0..100.0 &&
               packet.humidity in 0..100 &&
               packet.city.isNotBlank() &&
               packet.description.isNotBlank()
    }
}