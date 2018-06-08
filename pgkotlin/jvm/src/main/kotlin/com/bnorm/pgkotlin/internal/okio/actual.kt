package com.bnorm.pgkotlin.internal.okio

actual typealias BufferedSource = okio.BufferedSource
actual typealias BufferedSink = okio.BufferedSink
actual typealias Buffer = okio.Buffer
actual typealias ByteString = okio.ByteString

actual typealias IOException = java.io.IOException

actual fun encodeUtf8(s: String): ByteString = ByteString.encodeUtf8(s)
