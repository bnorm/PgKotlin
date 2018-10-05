package com.bnorm.pgkotlin.internal.okio

expect interface BufferedSink {
  fun writeByte(b: Int): BufferedSink
  fun writeShort(s: Int): BufferedSink
  fun writeInt(i: Int): BufferedSink
  fun writeUtf8(string: String): BufferedSink
  fun write(byteString: ByteString): BufferedSink
  fun write(source: Buffer, byteCount: Long)
  fun emit(): BufferedSink
}
