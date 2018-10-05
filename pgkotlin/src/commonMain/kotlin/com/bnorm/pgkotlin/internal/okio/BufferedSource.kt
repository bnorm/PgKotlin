package com.bnorm.pgkotlin.internal.okio

expect interface BufferedSource {
  fun readByte(): Byte
  fun readShort(): Short
  fun readInt(): Int
  fun readUtf8(byteCount: Long): String
  fun readByteString(): ByteString
  fun readByteString(byteCount: Long): ByteString
  fun indexOf(b: Byte): Long
  fun skip(byteCount: Long)
}
