package com.bnorm.pgkotlin.internal.msg

import okio.Buffer
import okio.BufferedSink
import okio.BufferedSource
import java.io.IOException

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

internal fun BufferedSource.readUtf8Terminated(): String {
  val index = indexOf(0)
  if (index != -1L) {
    return readUtf8(index).also { skip(1) }
  } else {
    throw IOException("Unterminated string")
  }
}

internal fun BufferedSink.writeUtf8Terminated(utf8: String) {
  writeUtf8(utf8)
  writeByte(0)
}

internal fun Request.writeTo(sink: BufferedSink) {
  if (id > 0) sink.writeByte(id)
  val buffer = Buffer()
  encode(buffer)
  sink.writeInt(buffer.size().toInt() + 4)
  sink.writeAll(buffer)
  sink.emit()
}
