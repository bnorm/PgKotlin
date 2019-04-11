package com.bnorm.pgkotlin.internal.msg

import com.bnorm.pgkotlin.internal.okio.Buffer
import com.bnorm.pgkotlin.internal.okio.BufferedSink
import com.bnorm.pgkotlin.internal.okio.BufferedSource
import com.bnorm.pgkotlin.internal.okio.IOException

internal val factories = listOf<Message.Factory<*>>(
  Authentication,
  BackendKeyData,
  BindComplete,
  CloseComplete,
  CommandComplete,
  DataRow,
  EmptyQueryResponse,
  ErrorResponse,
  NoData,
  NoticeResponse,
  NotificationResponse,
  ParameterDescription,
  ParameterStatus,
  ParseComplete,
  PortalSuspended,
  ReadyForQuery,
  RowDescription
).associateBy { it.id }

internal interface Message {
  val id: Int

  interface Factory<out M : Message> {
    val id: Int

    fun decode(source: BufferedSource): M
  }
}

internal interface Request : Message {
  fun encode(sink: BufferedSink)

  fun writeTo(sink: BufferedSink) {
    if (id > 0) sink.writeByte(id)
    val buffer = Buffer()
    encode(buffer)
    sink.writeInt(buffer.size.toInt() + 4)
    sink.write(buffer, buffer.size)
  }
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
