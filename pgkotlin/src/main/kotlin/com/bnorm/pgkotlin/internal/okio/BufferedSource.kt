package com.bnorm.pgkotlin.internal.okio

expect interface BufferedSource {
  fun readByte(): Byte
  fun readInt(): Int
  fun readUtf8(byteCount: Long): String
  fun indexOf(b: Byte): Long
  fun skip(byteCount: Long)
}
