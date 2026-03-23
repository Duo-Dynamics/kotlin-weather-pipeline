package com.weatherflow.main

import com.weatherflow.auth.AuthService
import com.weatherflow.consumer.WeatherConsumer
import com.weatherflow.filter.FilterServer
import com.weatherflow.model.WeatherPacket
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.routing.*
import io.ktor.server.websocket.*
import io.ktor.websocket.*
import kotlinx.coroutines.*
import java.io.FileInputStream
import java.util.Properties
import java.util.concurrent.CopyOnWriteArrayList
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper

val sessions = CopyOnWriteArrayList<DefaultWebSocketSession>()
val mapper = jacksonObjectMapper()

fun main() {
    // Load secret key from local.properties
    val props = Properties()
    props.load(FileInputStream("local.properties"))
    val secret = props["hmac.secret"] as String

    // Start AuthService in background thread
    Thread {
        AuthService(secret).start()
    }.start()

    // Start FilterServer in background thread
    Thread {
        FilterServer().start()
    }.start()

    // Start WeatherConsumer — pushes packets to all WebSocket sessions
    Thread {
        WeatherConsumer { packet ->
            val json = mapper.writeValueAsString(packet)
            runBlocking {
                for (session in sessions) {
                    try {
                        session.send(Frame.Text(json))
                    } catch (e: Exception) {
                        sessions.remove(session)
                    }
                }
            }
        }.start()
    }.start()

    // Start WebSocket server on port 8080
    embeddedServer(Netty, port = 8080) {
        install(WebSockets)
        routing {
            webSocket("/weather") {
                sessions.add(this)
                println("[WebSocket] Android client connected")
                try {
                    for (frame in incoming) { /* keep alive */ }
                } finally {
                    sessions.remove(this)
                    println("[WebSocket] Android client disconnected")
                }
            }
        }
    }.start(wait = true)
}