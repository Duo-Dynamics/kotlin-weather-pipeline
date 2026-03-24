package com.weatherflow.producer

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.weatherflow.model.WeatherPacket
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.Properties
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

fun createProducer(): KafkaProducer<String, String> {
    val props = Properties()
    props["bootstrap.servers"] = "localhost:9092"
    props["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
    props["value.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
    return KafkaProducer(props)
}

fun signPacket(packet: WeatherPacket, secretKey: String): WeatherPacket {
    val data = "${packet.city}${packet.temperature}${packet.humidity}${packet.description}${packet.timestamp}"
    val mac = Mac.getInstance("HmacSHA256")
    val keySpec = SecretKeySpec(secretKey.toByteArray(), "HmacSHA256")
    mac.init(keySpec)
    val signature = mac.doFinal(data.toByteArray())
        .joinToString("") { "%02x".format(it) }
    return packet.copy(signature = signature)
}

fun sendToKafka(producer: KafkaProducer<String, String>, packet: WeatherPacket) {
    val mapper = ObjectMapper().registerKotlinModule()
    val json = mapper.writeValueAsString(packet)
    val record = ProducerRecord<String, String>("raw-weather", packet.city, json)
    producer.send(record)
    producer.flush()
}