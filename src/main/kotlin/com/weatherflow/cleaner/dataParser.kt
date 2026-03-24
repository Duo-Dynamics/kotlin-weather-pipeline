package com.weatherflow.cleaner

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.weatherflow.model.WeatherPacket

fun parseWeatherData(rawJson: String): WeatherPacket {
    val mapper = ObjectMapper().registerKotlinModule()
    val root = mapper.readTree(rawJson)

    return WeatherPacket(
        city = root["name"]?.asText() ?: throw Exception("Missing city name"),
        temperature = root["main"]?.get("temp")?.asDouble() ?: throw Exception("Missing temperature"),
        humidity = root["main"]?.get("humidity")?.asInt() ?: throw Exception("Missing humidity"),
        description = root["weather"]?.get(0)?.get("description")?.asText() ?: throw Exception("Missing description"),
        timestamp = System.currentTimeMillis(),
        signature = ""
    )
}