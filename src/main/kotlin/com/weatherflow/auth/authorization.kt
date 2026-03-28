package com.weatherflow.auth

import org.apache.kafka.clients.producer.KafkaProducer
import com.weatherflow.model.WeatherPacket
import com.weatherflow.config.Topics
import com.weatherflow.kafka.sendToKafka
import com.weatherflow.crypto.computeHmac

private fun verifySignature(packet: WeatherPacket, secretKey: String): Boolean{
    val data = "${packet.city}${packet.temperature}${packet.humidity}${packet.description}${packet.timestamp}"
    return packet.signature == computeHmac(data, secretKey)
}

fun writeAfterAuth(packet: WeatherPacket, producer: KafkaProducer<String, String>, secretKey: String) {
    try {
        val topic = if (verifySignature(packet, secretKey)) Topics.VERIFIED_WEATHER else Topics.AUTH_FAILED
        sendToKafka(producer, packet, topic)
    } catch (e: Exception) {
        println("Failed to process packet for ${packet.city}: ${e.message}")
    }
}