package com.weatherflow.fetcher

import okhttp3.OkHttpClient
import okhttp3.Request
import java.util.Properties
import java.io.FileInputStream

fun fetchWeather(city: String): String {

    val properties = Properties()
    properties.load(FileInputStream("local.properties"))
    val apiKey = properties.getProperty("WEATHER_API_KEY")

    val url =
        "https://api.openweathermap.org/data/2.5/weather?q=$city&APPID=$apiKey&units=metric"

    val client = OkHttpClient()

    val request = Request.Builder()
        .url(url)
        .get()
        .build()

    val response = client.newCall(request).execute()

    if (!response.isSuccessful) {
        throw Exception("HTTP Error: ${response.code}")
    }

    return response.body?.string() ?: ""
}