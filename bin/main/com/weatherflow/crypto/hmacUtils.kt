package com.weatherflow.crypto

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

fun computeHmac(data: String, secretKey: String): String {
    val mac = Mac.getInstance("HmacSHA256")
    val keySpec = SecretKeySpec(secretKey.toByteArray(), "HmacSHA256")
    mac.init(keySpec)
    return mac.doFinal(data.toByteArray()).joinToString("") { "%02x".format(it) }
}