package com.weatherflow.producer

import com.weatherflow.model.WeatherPacket
import com.weatherflow.kafka.sendToKafka
import org.apache.kafka.clients.producer.KafkaProducer
import com.weatherflow.config.Topics
import com.weatherflow.crypto.computeHmac

private fun signPacket(packet: WeatherPacket, secretKey: String): WeatherPacket {
    val data = "${packet.city}${packet.temperature}${packet.humidity}${packet.description}${packet.timestamp}"
    val signature = computeHmac(data, secretKey)
    return packet.copy(signature = signature)
}

fun produceWeather(packet: WeatherPacket, secretKey: String, producer: KafkaProducer<String, String>){
    val signed = signPacket(packet, secretKey)
    sendToKafka(producer, signed, Topics.RAW_WEATHER)
}