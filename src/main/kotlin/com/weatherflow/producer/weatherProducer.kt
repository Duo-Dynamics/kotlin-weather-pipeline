package com.weatherflow.producer

import com.weatherflow.model.WeatherPacket
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import com.weatherflow.kafka.createProducer
import com.weatherflow.kafka.sendToKafka

fun signPacket(packet: WeatherPacket, secretKey: String): WeatherPacket {
    val data = "${packet.city}${packet.temperature}${packet.humidity}${packet.description}${packet.timestamp}"
    val mac = Mac.getInstance("HmacSHA256")
    val keySpec = SecretKeySpec(secretKey.toByteArray(), "HmacSHA256")
    mac.init(keySpec)
    val signature = mac.doFinal(data.toByteArray())
        .joinToString("") { "%02x".format(it) }
    return packet.copy(signature = signature)
}