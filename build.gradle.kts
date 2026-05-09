plugins {
    kotlin("jvm") version "1.9.22"
    application
}

group = "com.weatherflow"
version = "1.0.0"

val ktor_version = "2.3.9"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.kafka:kafka-clients:4.0.0")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.17.0")
    implementation("org.slf4j:slf4j-simple:2.0.12")
    implementation("com.squareup.okhttp3:okhttp:4.12.0")

    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.0")

    implementation("io.ktor:ktor-server-core:$ktor_version")
    implementation("io.ktor:ktor-server-netty:$ktor_version")
    implementation("io.ktor:ktor-server-websockets:$ktor_version")
}

application {
    mainClass.set("com.weatherflow.main.MainKt")
}