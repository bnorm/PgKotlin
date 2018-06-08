package com.bnorm.pgkotlin.internal.okio

expect class ByteString {
  fun size(): Int
  fun md5(): ByteString
  fun hex(): String
}

expect fun encodeUtf8(s: String): ByteString
