package com.weatherflow.cleaner

import com.weatherflow.model.WeatherPacket

fun cleanWeatherData(packet: WeatherPacket): WeatherPacket {
    require(packet.city.isNotBlank()) { "City name cannot be empty" }
    require(packet.temperature in -100.0..100.0) { "Temperature out of range: ${packet.temperature}" }
    require(packet.humidity in 0..100) { "Humidity out of range: ${packet.humidity}" }
    require(packet.description.isNotBlank()) { "Description cannot be empty" }
    
    return packet
}