package com.weatherflow.filters

import org.apache.kafka.clients.producer.KafkaProducer
import com.weatherflow.model.WeatherPacket
import com.weatherflow.config.Topics
import com.weatherflow.kafka.sendToKafka
import com.weatherflow.cleaner.cleanWeatherData

fun writeAfterVerification(packet: WeatherPacket, producer: KafkaProducer<String, String>) {
    try {
        val validPacket = cleanWeatherData(packet)
        sendToKafka(producer, validPacket, Topics.CLEAN_WEATHER)
    } catch (e: IllegalArgumentException) {
        sendToKafka(producer, packet, Topics.FILTER_REJECTED)
    } catch (e: Exception) {
        println("Unexpected error for ${packet.city}: ${e.message}")
    }
}