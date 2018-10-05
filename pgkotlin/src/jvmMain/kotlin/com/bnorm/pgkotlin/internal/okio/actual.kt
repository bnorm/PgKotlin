package com.bnorm.pgkotlin.internal.okio

import okio.ByteString.Companion.encodeUtf8
import okio.ByteString.Companion.toByteString

actual typealias BufferedSource = okio.BufferedSource
actual typealias BufferedSink = okio.BufferedSink
actual typealias Buffer = okio.Buffer
actual typealias ByteString = okio.ByteString

actual typealias IOException = java.io.IOException

actual fun ofByteString(vararg bytes: Byte): ByteString = bytes.toByteString()
actual fun encodeUtf8(s: String): ByteString = s.encodeUtf8()
