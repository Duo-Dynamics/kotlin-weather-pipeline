package com.weatherflow.main

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

fun main() {
    val secret = "supersecretkey123"
    val city = "Lahore"
    val temperature = 32.5
    val humidity = 60
    val description = "clear sky"
    val timestamp = System.currentTimeMillis()

    val data = "$city|$temperature|$humidity|$description|$timestamp"

    val mac = Mac.getInstance("HmacSHA256")
    val keySpec = SecretKeySpec(secret.toByteArray(), "HmacSHA256")
    mac.init(keySpec)
    val signature = mac.doFinal(data.toByteArray())
        .joinToString("") { "%02x".format(it) }

    println("timestamp: $timestamp")
    println("signature: $signature")
    println()
    println("JSON to paste into Kafka:")
    println("""
        {
          "city": "$city",
          "temperature": $temperature,
          "humidity": $humidity,
          "description": "$description",
          "timestamp": $timestamp,
          "signature": "$signature"
        }
    """.trimIndent())
}