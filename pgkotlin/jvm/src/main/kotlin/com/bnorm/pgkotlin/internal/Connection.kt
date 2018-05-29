package com.bnorm.pgkotlin.internal

import com.bnorm.pgkotlin.NotificationChannel
import com.bnorm.pgkotlin.QueryExecutor
import com.bnorm.pgkotlin.Response
import com.bnorm.pgkotlin.Transaction
import com.bnorm.pgkotlin.internal.msg.*
import com.bnorm.pgkotlin.internal.protocol.Postgres10
import com.bnorm.pgkotlin.internal.protocol.Protocol
import kotlinx.coroutines.experimental.channels.*
import kotlinx.coroutines.experimental.nio.aConnect
import kotlinx.coroutines.experimental.nio.aRead
import kotlinx.coroutines.experimental.nio.aWrite
import kotlinx.coroutines.experimental.runBlocking
import okio.Buffer
import org.intellij.lang.annotations.Language
import java.io.Closeable
import java.io.IOException
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousSocketChannel

internal class Connection(
  private val protocol: Protocol,
  private val channels: MutableMap<String, BroadcastChannel<String>>
) : QueryExecutor, NotificationChannel, Closeable {

  override suspend fun listen(channel: String): BroadcastChannel<String> {
    val broadcast = channels.computeIfAbsent(channel) {
      val delegate = BroadcastChannel<String>(Channel.CONFLATED)
      return@computeIfAbsent object : BroadcastChannel<String> by delegate {
        override fun close(cause: Throwable?): Boolean {
          channels.remove(channel)
          var actual = cause
          try {
            runBlocking {
              query("UNLISTEN $channel")
            }
          } catch (t: Throwable) {
            when (actual) {
              null -> actual = t
              else -> actual.addSuppressed(t)
            }
          }
          return delegate.close(actual)
        }
      }
    }
    query("LISTEN $channel")
    return broadcast
  }

  override suspend fun begin(): Transaction {
    query("BEGIN TRANSACTION")
    return PgTransaction(this)
  }

  suspend fun stream(
    @Language("PostgreSQL") sql: String,
    vararg params: Any?,
    rows: Int = 5
  ): Response {
    val portal = if (params.isEmpty()) protocol.simpleQuery(sql)
    else protocol.extendedQuery(sql, params.toList(), rows)

    return if (portal == null) Response.Empty
    else Response.Stream(portal)
  }

  override suspend fun query(
    @Language("PostgreSQL") sql: String,
    vararg params: Any?
  ) = stream(sql, params = *params, rows = 0)

  override fun close() {
    runBlocking {
      protocol.terminate()
    }
  }

  companion object {
    private val factories = listOf<Message.Factory<*>>(
      Authentication,
      BackendKeyData,
      BindComplete,
      CloseComplete,
      CommandComplete,
      DataRow,
      EmptyQueryResponse,
      ErrorResponse,
      NotificationResponse,
      ParameterDescription,
      ParameterStatus,
      ParseComplete,
      PortalSuspended,
      ReadyForQuery,
      RowDescription
    ).associateBy { it.id }

    suspend fun connect(
      hostname: String = "localhost",
      port: Int = 5432,
      username: String = "postgres",
      password: String? = null,
      database: String = "postgres"
    ) = connect(InetSocketAddress(hostname, port), username, password, database)

    suspend fun connect(
      address: InetSocketAddress = InetSocketAddress("localhost", 5432),
      username: String = "postgres",
      password: String? = null,
      database: String = "postgres"
    ): Connection {
      val socket = AsynchronousSocketChannel.open()
      socket.aConnect(address)

      val requests = sink(socket)
      val channels = mutableMapOf<String, BroadcastChannel<String>>()
      val responses = source(socket, channels)

      val protocol = Postgres10(requests, responses)

      protocol.startup(username, password, database)

      return Connection(protocol, channels)
    }

    private fun sink(socket: AsynchronousSocketChannel) = actor<Request> {
      socket.use { socket ->
        val buffer = Buffer()
        val cursor = Buffer.UnsafeCursor()

        for (msg in channel) {
          msg.writeTo(buffer)
          debug { println("sending=$msg") }
          socket.aWrite(buffer, buffer.size(), cursor)
        }
      }
    }

    private fun source(
      socket: AsynchronousSocketChannel,
      channels: MutableMap<String, BroadcastChannel<String>>
    ) = produce<Message> {
      val buffer = Buffer()
      val cursor = Buffer.UnsafeCursor()

      while (socket.isOpen && isActive) {
        while (buffer.size() < 5) socket.aRead(buffer, cursor)

        val id = buffer.readByte()
        val length = (buffer.readInt() - 4).toLong()
        while (buffer.size() < length) socket.aRead(buffer, cursor)

        val msg = factories[id.toInt()]?.decode(buffer)
        debug { println("received=$msg") }
        when {
          msg is ErrorResponse -> throw IOException("${msg.level}: ${msg.message} (${msg.code})")
          msg is NotificationResponse -> channels[msg.channel]?.send(msg.payload)
          msg != null -> send(msg)
          else -> {
            println("    unknown=${id.toChar()}")
            buffer.skip(length)
          }
        }
      }
    }
  }
}

private suspend fun AsynchronousSocketChannel.aRead(
  buffer: Buffer,
  cursor: Buffer.UnsafeCursor
): Long {
  buffer.readAndWriteUnsafe(cursor).use {
    val oldSize = buffer.size()
    cursor.expandBuffer(8192)

    val read = aRead(ByteBuffer.wrap(cursor.data, cursor.start, 8192))

    if (read == -1) {
      cursor.resizeBuffer(oldSize)
    } else {
      cursor.resizeBuffer(oldSize + read)
    }
    return read.toLong()
  }
}

suspend fun AsynchronousSocketChannel.aWrite(
  buffer: Buffer,
  byteCount: Long,
  cursor: Buffer.UnsafeCursor = Buffer.UnsafeCursor()
) {
  var remaining = byteCount
  while (remaining > 0) {
    buffer.readUnsafe(cursor).use {
      cursor.seek(0)

      val length = minOf(cursor.end - cursor.start, remaining.toInt())
      val written = aWrite(ByteBuffer.wrap(cursor.data, cursor.start, length))
      remaining -= written
      buffer.skip(written.toLong())
    }
  }
}
