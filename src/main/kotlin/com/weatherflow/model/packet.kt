package com.weatherflow.model

data class WeatherPacket(
    val city: String,
    val temperature: Double,
    val humidity: Int,
    val description: String,
    val timestamp: Long,
    val signature: String
)