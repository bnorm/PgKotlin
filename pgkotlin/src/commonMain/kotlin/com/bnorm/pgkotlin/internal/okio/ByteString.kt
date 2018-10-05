package com.bnorm.pgkotlin.internal.okio

expect class ByteString {
  val size: Int
  fun md5(): ByteString
  fun hex(): String
  fun utf8(): String
  operator fun get(index: Int): Byte
}

expect fun ofByteString(vararg bytes: Byte): ByteString
expect fun encodeUtf8(s: String): ByteString
