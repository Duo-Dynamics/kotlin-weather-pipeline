package com.weatherflow.main

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.weatherflow.auth.AuthService
import com.weatherflow.consumer.WeatherConsumer
import com.weatherflow.filter.FilterServer
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.routing.*
import io.ktor.server.websocket.*
import io.ktor.websocket.*
import kotlinx.coroutines.runBlocking
import java.io.FileInputStream
import java.util.Properties
import java.util.concurrent.CopyOnWriteArrayList

// keeps track of all connected Android clients
val sessions = CopyOnWriteArrayList<DefaultWebSocketSession>()
val mapper = jacksonObjectMapper()

fun main() {

    // load secret key from local.properties
    val props = Properties()
    props.load(FileInputStream("local.properties"))
    val secret = props["hmac.secret"] as String

    // start AuthService in background
    Thread {
        AuthService(secret).start()
    }.start()

    // start FilterServer in background
    Thread {
        FilterServer().start()
    }.start()

    // start WeatherConsumer in background
    // when packet arrives → push to all connected Android clients
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

    // start WebSocket server on port 8080
    embeddedServer(Netty, port = 8080) {
        install(WebSockets)
        routing {
            webSocket("/weather") {
                sessions.add(this)
                println("[WebSocket] ✅ Android client connected")
                try {
                    for (frame in incoming) {
                        // just keep connection alive
                    }
                } finally {
                    sessions.remove(this)
                    println("[WebSocket] ❌ Android client disconnected")
                }
            }
        }
    }.start(wait = true)
}