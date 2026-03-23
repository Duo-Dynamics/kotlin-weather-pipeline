plugins {
    kotlin("jvm") version "1.9.22"
    application
}

group = "com.weatherflow"
version = "1.0.0"

repositories {
    mavenCentral()
}

val ktorVersion = "2.3.9"

dependencies {
    implementation("org.apache.kafka:kafka-clients:4.0.0")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.17.0")
    implementation("org.slf4j:slf4j-simple:2.0.12")

    // Ktor WebSocket server
    implementation("io.ktor:ktor-server-core:$ktorVersion")
    implementation("io.ktor:ktor-server-netty:$ktorVersion")
    implementation("io.ktor:ktor-server-websockets:$ktorVersion")
}

application {
    mainClass.set("com.weatherflow.main.MainKt")
}