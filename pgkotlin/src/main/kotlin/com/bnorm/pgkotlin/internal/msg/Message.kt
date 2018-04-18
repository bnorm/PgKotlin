package com.bnorm.pgkotlin.internal.msg

import okio.Buffer
import okio.BufferedSink
import okio.BufferedSource
import kotlin.coroutines.experimental.buildSequence

internal interface Message {
  val id: Int

  interface Factory<out M : Message> {
    val id: Int

    fun decode(source: BufferedSource): M
  }
}

internal interface Request : Message {
  fun encode(sink: BufferedSink)
}

internal fun BufferedSource.readTerminatedString(): String {
  val iterator: Sequence<Byte> = buildSequence {
    var byte: Byte = readByte()
    while (byte != 0.toByte()) {
      yield(byte)
      byte = readByte()
    }
  }
  return String(iterator.toList().toByteArray())
}


internal fun Request.writeTo(sink: BufferedSink) {
  if (id > 0) sink.writeByte(id)
  val buffer = Buffer()
  encode(buffer)
  sink.writeInt(buffer.size().toInt() + 4)
  sink.writeAll(buffer)
  sink.emit()
}