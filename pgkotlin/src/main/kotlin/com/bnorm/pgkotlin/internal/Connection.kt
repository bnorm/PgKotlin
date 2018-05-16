package com.bnorm.pgkotlin.internal

import com.bnorm.pgkotlin.NotificationChannel
import com.bnorm.pgkotlin.QueryExecutor
import com.bnorm.pgkotlin.Response
import com.bnorm.pgkotlin.Transaction
import com.bnorm.pgkotlin.internal.msg.*
import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.*
import kotlinx.coroutines.experimental.nio.aConnect
import kotlinx.coroutines.experimental.nio.aRead
import kotlinx.coroutines.experimental.nio.aWrite
import okio.Buffer
import org.intellij.lang.annotations.Language
import java.io.Closeable
import java.io.IOException
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousSocketChannel
import java.util.concurrent.TimeUnit

internal class Connection(
  private val requests: SendChannel<Request>,
  private val responses: ReceiveChannel<Message>,
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
    if (params.isEmpty()) {
      requests.send(Query(sql))
    } else {
      requests.send(Parse(sql))
      requests.send(Bind(params.map { it.pgEncode() }))
      requests.send(Describe(StatementType.Portal))
      requests.send(Execute(rows = rows))
      requests.send(Sync)

      responses.require<ParseComplete>()
      responses.require<BindComplete>()
    }

    val first = responses.receive()
    when (first) {
      is CommandComplete -> {
        responses.require<ReadyForQuery>()
        return Response.Empty
      }
      is EmptyQueryResponse -> {
        responses.require<ReadyForQuery>()
        return Response.Empty
      }
      is RowDescription -> {
        // Buffer 1 less than the number of possible rows to keep additional
        // executions from being sent
        return Response.Stream(object : Portal(first, produce<DataRow>(
          capacity = (rows - 1).coerceAtLeast(0),
          context = Unconfined
        ) {
          for (msg in responses) {
            when (msg) {
              is DataRow -> {
                send(msg)
              }
              is PortalSuspended -> {
                responses.require<ReadyForQuery>()
                requests.send(Execute(rows = rows))
                requests.send(Sync)
              }
              is CommandComplete -> {
                responses.require<ReadyForQuery>()
                requests.send(Close(StatementType.Portal))
                requests.send(Sync)
                responses.require<CloseComplete>()
                responses.require<ReadyForQuery>()
                return@produce
              }
              else -> throw PgProtocolException("msg=$msg")
            }
          }
        }) {
          override suspend fun close() {
            // Cancel production and close the portal
            // Consume messages until the confirmation of portal closure
            if (!isClosedForReceive) {
              cancel()
              withContext(NonCancellable) {
                requests.send(Close(StatementType.Portal))
                requests.send(Sync)
                responses.consumeUntil<CloseComplete>()
                responses.require<ReadyForQuery>()
              }
            }
          }
        })
      }
      else -> throw PgProtocolException("msg=$first")
    }
  }

  override suspend fun query(
    @Language("PostgreSQL") sql: String,
    vararg params: Any?
  ): Response {
    if (params.isEmpty()) {
      requests.send(Query(sql))
    } else {
      requests.send(Parse(sql))
      requests.send(Bind(params.map { it.pgEncode() }))
      requests.send(Describe(StatementType.Portal))
      requests.send(Execute())
      requests.send(Sync)

      responses.require<ParseComplete>()
      responses.require<BindComplete>()
    }

    var columns: RowDescription? = null
    val rows = mutableListOf<DataRow>()
    for (msg in responses) {
      if (msg is CommandComplete) break
      when (msg) {
        is RowDescription -> columns = msg
        is DataRow -> rows.add(msg)
        else -> throw PgProtocolException("msg=$msg")
      }
    }

    responses.require<ReadyForQuery>()

    return when (columns) {
      null -> Response.Empty
      else -> Response.Complete(columns, rows)
    }
  }

  override fun close() {
    runBlocking {
      requests.send(Terminate)
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

      // TODO(bnorm) SSL handshake?
      requests.send(StartupMessage(username = username, database = database))

      val authentication = responses.receive() as? Authentication ?: throw PgProtocolException()
      if (!authentication.success) {
        if (password != null) {
          requests.send(PasswordMessage.create(username, password, authentication.md5salt))
        } else {
          throw IllegalArgumentException("no authentication")
        }
      }

      // responses consume BackendKeyData
      // responses consume ParameterStatus

      responses.consumeUntil<ReadyForQuery>()

      return Connection(requests, responses, channels)
    }

    private fun sink(socket: AsynchronousSocketChannel) = actor<Request> {
      socket.use { socket ->
        val buffer = Buffer()
        val cursor = Buffer.UnsafeCursor()

        for (msg in channel) {
          msg.writeTo(buffer)
          // println("sending=$msg")
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
        // println("received=$msg")
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

private suspend inline fun <reified T> ReceiveChannel<Message>.require(): T {
  val msg = withTimeout(30, TimeUnit.SECONDS) { receive() }
  return msg as? T ?: throw PgProtocolException("unexpected=$msg")
}

private suspend inline fun <reified T : Message> ReceiveChannel<Message>.consumeUntil(): T {
  for (msg in this) {
    if (msg is T) {
      return msg
    }
  }
  throw PgProtocolException("not found = ${T::class}")
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
