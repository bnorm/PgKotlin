package com.bnorm.pgkotlin.internal

import com.bnorm.pgkotlin.internal.msg.ErrorResponse
import com.bnorm.pgkotlin.internal.msg.Message
import com.bnorm.pgkotlin.internal.msg.ReadyForQuery
import com.bnorm.pgkotlin.internal.msg.Request
import com.bnorm.pgkotlin.internal.okio.Buffer
import com.bnorm.pgkotlin.internal.okio.BufferedSink
import com.bnorm.pgkotlin.internal.okio.IOException
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel

internal interface PostgresSessionFactory {
  suspend fun create(): PostgresSession
}

internal suspend inline fun <R> PostgresSessionFactory.session(block: PostgresSession.() -> R): R {
  val session = create()
  return session.block()
}

internal interface PostgresSession {
  suspend fun receive(): Message
  suspend fun send(msg: Request)
}

internal class ChannelPostgresSession(
  private val requests: SendChannel<Request>,
  private val responses: ReceiveChannel<Message>
) : PostgresSession {
  override suspend fun receive(): Message {
    val msg = responses.receive()
    if (msg is ErrorResponse) {
      var ready = responses.receive()
      while (ready !is ReadyForQuery) {
        println("after an error: $ready") // TODO
        ready = responses.receive()
      }
      throw IOException("${msg.level} (${msg.code}): ${msg.message}")
    }
    return msg
  }

  override suspend fun send(msg: Request) {
    requests.send(msg)
  }
}

internal suspend inline fun <reified T : Message> PostgresSession.receive(): T {
  val msg = receive()
  return msg as? T ?: throw PgProtocolException("unexpected=$msg")
}

internal suspend inline fun <reified T : Message> PostgresSession.receiveUntil(block: (msg: Message) -> Unit) {
  var msg: Message = receive()
  while (msg !is T) {
    block(msg)
    msg = receive()
  }
}

internal suspend fun PostgresSession.send(vararg requests: Request) {
  class RequestBundle : Request {
    override val id = -1

    override fun encode(sink: BufferedSink) {
    }

    override fun writeTo(sink: BufferedSink) {
      val buffer = Buffer()
      for (request in requests) {
        with(request) {
          if (id > 0) sink.writeByte(id)
          encode(buffer)
          sink.writeInt(buffer.size.toInt() + 4)
          sink.write(buffer, buffer.size)
        }
      }
    }
  }

  send(RequestBundle())
}
