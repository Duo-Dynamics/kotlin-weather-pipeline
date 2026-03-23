package com.weatherflow.auth

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
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

class AuthService(private val secretKey: String) {

    private val mapper = jacksonObjectMapper()

    // reads from raw-weather topic
    private val consumer = KafkaConsumer<String, String>(Properties().apply {
        put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        put(ConsumerConfig.GROUP_ID_CONFIG, "auth-service-group")
        put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    })

    // writes to verified-weather or auth-failed
    private val producer = KafkaProducer<String, String>(Properties().apply {
        put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
    })

    fun start() {
        consumer.subscribe(listOf("raw-weather"))
        println("[AuthService] Listening on raw-weather...")
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
            println("[AuthService] Failed to deserialize: ${e.message}")
            return
        }

        val expectedSignature = computeHmac(packet)

        if (expectedSignature == packet.signature) {
            println("[AuthService] ✅ Verified: ${packet.city}")
            producer.send(ProducerRecord("verified-weather", packet.city, json))
        } else {
            println("[AuthService] ❌ Auth failed: ${packet.city}")
            producer.send(ProducerRecord("auth-failed", packet.city, json))
        }
    }

    private fun computeHmac(packet: WeatherPacket): String {
        val data = "${packet.city}|${packet.temperature}|${packet.humidity}|${packet.description}|${packet.timestamp}"
        val mac = Mac.getInstance("HmacSHA256")
        val keySpec = SecretKeySpec(secretKey.toByteArray(Charsets.UTF_8), "HmacSHA256")
        mac.init(keySpec)
        return mac.doFinal(data.toByteArray(Charsets.UTF_8))
            .joinToString("") { "%02x".format(it) }
    }
}