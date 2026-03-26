package com.weatherflow.kafka

import java.util.Properties
import java.io.FileInputStream
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.weatherflow.model.WeatherPacket

fun createProducer(): KafkaProducer<String, String> {
    val props = Properties()
    props.load(FileInputStream("local.properties"))
    props["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
    props["value.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
    return KafkaProducer(props)
}

fun createConsumer(group_id: String): KafkaConsumer<String, String>{
    val props = Properties()
    props.load(FileInputStream("local.properties"))
    props["group.id"] = group_id
    props["key.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
    props["value.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
    return KafkaConsumer(props)
}

fun sendToKafka(producer: KafkaProducer<String, String>, packet: WeatherPacket, topic: String) {
    val mapper = ObjectMapper().registerKotlinModule()
    val json = mapper.writeValueAsString(packet)
    val record = ProducerRecord<String, String>(topic, packet.city, json)
    producer.send(record)
    producer.flush()
}