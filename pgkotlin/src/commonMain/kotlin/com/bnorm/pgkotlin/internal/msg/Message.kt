package com.bnorm.pgkotlin.internal.msg

import kotlinx.coroutines.channels.*
import kotlinx.io.core.*

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
  NotificationResponse,
  ParameterDescription,
  ParameterStatus,
  ParseComplete,
  PortalSuspended,
  ReadyForQuery,
  RowDescription
).associateBy { it.id }

internal interface Message {
  val id: Byte

  interface Factory<out M : Message> {
    val id: Byte

    fun decode(source: Input): M
  }
}

internal interface Request : Message {
  fun encode(sink: Output)

  fun writeTo(sink: Output) {
    if (id > 0) sink.writeByte(id)
    val packet = buildPacket {
      encode(this)
    }
    sink.writeInt(packet.remaining.toInt() + 4)
    sink.writePacket(packet)
  }
}

internal suspend fun SendChannel<Request>.send(vararg requests: Request) {
  send(RequestBundle(requests.toList()))
}

private data class RequestBundle(
  val requests: List<Request>
) : Request {
  override val id = (-1).toByte()

  override fun encode(sink: Output) {
  }

  override fun writeTo(sink: Output) {
    BytePacketBuilder(0).use { builder ->
      for (request in requests) {
        with(request) {
          if (id > 0) sink.writeByte(id)
          encode(builder)
          val packet = builder.build()
          sink.writeInt(packet.remaining.toInt() + 4)
          sink.writePacket(packet)
        }
      }
    }
  }
}

internal fun Input.readUtf8Terminated(): String {
  // TODO there's got to be a better way to read the string...
  return readUTF8UntilDelimiter(0.toChar().toString()).also { discard(1) }
}

internal fun Output.writeUtf8Terminated(utf8: String) {
  append(utf8)
  writeByte(0)
}
