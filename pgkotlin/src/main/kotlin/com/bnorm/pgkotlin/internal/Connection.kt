package com.bnorm.pgkotlin.internal

import com.bnorm.pgkotlin.NotificationChannel
import com.bnorm.pgkotlin.QueryExecutor
import com.bnorm.pgkotlin.Response
import com.bnorm.pgkotlin.Transaction
import com.bnorm.pgkotlin.internal.msg.*
import kotlinx.coroutines.experimental.NonCancellable
import kotlinx.coroutines.experimental.channels.*
import kotlinx.coroutines.experimental.nio.aConnect
import kotlinx.coroutines.experimental.nio.aRead
import kotlinx.coroutines.experimental.nio.aWrite
import kotlinx.coroutines.experimental.runBlocking
import kotlinx.coroutines.experimental.withContext
import okio.Buffer
import org.intellij.lang.annotations.Language
import java.io.IOException
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousSocketChannel

internal class Connection(
  private val requests: SendChannel<Request>,
  private val responses: ReceiveChannel<Message>,
  private val channels: MutableMap<String, BroadcastChannel<String>>
) : QueryExecutor, NotificationChannel {

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
    vararg params: Any,
    rows: Int = 0
  ): Response {
    requests.send(Parse(sql))
    requests.send(Bind(params.map { it::class.toPgType().encode(it) }))
    requests.send(Describe(StatementType.Portal))
    requests.send(Execute(rows = rows))
    requests.send(Sync)

    responses.expect<ParseComplete>()
    responses.expect<BindComplete>()

    val first = responses.receive()
    when (first) {
      is CommandComplete -> {
        responses.expect<ReadyForQuery>()
        return Response.Empty
      }
      is EmptyQueryResponse -> {
        responses.expect<ReadyForQuery>()
        return Response.Empty
      }
      is RowDescription -> {
        // Buffer 1 less than the number of possible rows to keep additional
        // executions from being sent
        return Response.Stream(object : Portal(first, produce<DataRow>(
          capacity = (rows - 1).coerceAtLeast(0)
        ) {
          for (msg in responses) {
            when (msg) {
              is DataRow -> {
                send(msg)
              }
              is PortalSuspended -> {
                responses.expect<ReadyForQuery>()
                requests.send(Execute(rows = rows))
                requests.send(Sync)
              }
              is CommandComplete -> {
                responses.expect<ReadyForQuery>()
                requests.send(Close(StatementType.Portal))
                requests.send(Sync)
                responses.expect<CloseComplete>()
                responses.expect<ReadyForQuery>()
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
                responses.expect<ReadyForQuery>()
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
      requests.send(Describe(StatementType.Prepared))
      requests.send(Execute())
      requests.send(Close(StatementType.Prepared))
      requests.send(Sync)

      responses.expect<ParseComplete>()
      responses.expect<BindComplete>()
    }

    // TODO(bnorm): is there a way to stream this?
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

    if (params.isNotEmpty()) responses.expect<CloseComplete>()
    responses.expect<ReadyForQuery>()

    return when (columns) {
      null -> Response.Empty
      else -> Response.Complete(columns, rows)
    }
  }

  companion object {
    private val factories = listOf<Message.Factory<*>>(
      Authentication,
      BindComplete,
      CloseComplete,
      CommandComplete,
      DataRow,
      EmptyQueryResponse,
      ErrorResponse,
      NotificationResponse,
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

      val requests = actor<Request> {
        socket.use { socket ->
          val buffer = Buffer()
          for (msg in channel) {
            msg.writeTo(buffer)
//            println("sending=$msg")
            val len = buffer.size().toInt()
            val temp = ByteBuffer.allocate(len)
            buffer.read(temp)
            temp.flip()
            socket.aWrite(temp)
          }
        }
      }

      val channels = mutableMapOf<String, BroadcastChannel<String>>()

      val responses = produce<Message> {
        val buffer = Buffer()
        val cursor = Buffer.UnsafeCursor()

        while (socket.isOpen && isActive) {
          while (buffer.size() < 5) socket.aRead(buffer, cursor)

          val id = buffer.readByte()
          val length = (buffer.readInt() - 4).toLong()
          while (buffer.size() < length) socket.aRead(buffer, cursor)

          val msg = factories[id.toInt()]?.decode(buffer)
//          println("received=$msg")
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

      responses.receive() as? ReadyForQuery ?: throw PgProtocolException()

      return Connection(requests, responses, channels)
    }
  }
}

private suspend inline fun <reified T> ReceiveChannel<Message>.expect() {
  val msg = receive()
  msg as? T ?: throw PgProtocolException("unexpected=$msg")
}

private suspend inline fun <reified T : Message> ReceiveChannel<Message>.consumeUntil() {
  for (msg in this) {
    if (msg is T) {
      break
    }
  }
}

private suspend fun AsynchronousSocketChannel.aRead(
  buffer: Buffer,
  cursor: Buffer.UnsafeCursor
): Long {
  buffer.readAndWriteUnsafe(cursor).use {
    val oldSize = buffer.size()
    cursor.seek(oldSize)

    val length = 8192
    cursor.expandBuffer(length)
    val read = aRead(toByteBuffer(cursor, length))

    if (read == -1) {
      cursor.resizeBuffer(oldSize)
      return -1
    } else {
      cursor.resizeBuffer(oldSize + read)
      return read.toLong()
    }
  }
}

private fun toByteBuffer(
  cursor: Buffer.UnsafeCursor,
  length: Int
): ByteBuffer {
  require(length <= cursor.end - cursor.start)
  return ByteBuffer.wrap(cursor.data, cursor.start, length)
}
