package com.bnorm.pgkotlin.internal

import com.bnorm.pgkotlin.NotificationChannel
import com.bnorm.pgkotlin.QueryExecutor
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
import java.io.EOFException
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
  ): Portal? {
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
        return null
      }
      is EmptyQueryResponse -> {
        return null
      }
      is RowDescription -> {
        // Buffer 1 less than the number of possible rows to keep additional
        // executions being sent
        return object : Portal(first, produce<DataRow>(
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
        }
      }
      else -> throw PgProtocolException("msg=$first")
    }
  }

  override suspend fun query(
    @Language("PostgreSQL") sql: String,
    vararg params: Any
  ): Results {
    if (params.isEmpty()) {
      requests.send(Query(sql))
    } else {
      requests.send(Parse(sql))
      requests.send(Bind(params.map { it::class.toPgType().encode(it) }))
      requests.send(Describe(StatementType.Prepared))
      requests.send(Execute())
      requests.send(Close(StatementType.Prepared))
      requests.send(Sync)
    }

    // TODO(bnorm): is there a way to stream this?
    val columns = mutableListOf<Column<*>>()
    val rows = mutableListOf<com.bnorm.pgkotlin.internal.Row>()
    responses@ for (msg in responses) {
      if (msg is CommandComplete) {
        break
      }

      when (msg) {
        is RowDescription -> {
          for (column in msg.columns) {
            columns.add(Column(column.name, column.type))
          }
        }
        is DataRow -> {
          rows.add(com.bnorm.pgkotlin.internal.Row(msg.values.zip(columns) { value, column ->
            value?.let {
              try {
                column.type.decode(it)
              } catch (t: Throwable) {
                throw PgProtocolException(throwable = t)
              }
            }
          }))
        }
        else -> throw PgProtocolException("msg=$msg")
      }
    }

    responses.expect<ReadyForQuery>()
    return Results(columns, rows)
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
            println("sending=$msg")
            val len = buffer.size().toInt()
            val temp = ByteBuffer.allocate(len)
            buffer.read(temp)
            temp.flip()
            socket.aWrite(temp)
          }
        }
      }

      val channels = mutableMapOf<String, BroadcastChannel<String>>()

//      val reader = writer(coroutineContext) {
//        while (isActive && socket.isOpen) {
//          channel.writeSuspendSession {
//            val buffer = request(1)
//            if (buffer != null) {
//              val read = socket.aRead(buffer)
//              written(read)
//            }
//          }
//        }
//      }

//      val responses = produce<Message>(parent = reader) {
//        val buffer = Buffer()
//        val channel = reader.channel
//
//        while (isActive) {
//          val id = channel.readByte()
//          val length = channel.readInt()
////          val packet = channel.readPacket(length)
////          try {
////            packet.readFully()
////          } finally {
////            packet.release()
////          }
//          while (buffer.size() < length) {
//            channel.read(length) { b ->
//              val copy = b.asReadOnlyBuffer()
//              copy.limit((length - buffer.size()).toInt())
//              buffer.write(copy)
//            }
//          }
//
//          println("received = ${id.toChar()}")
//          val msg = factories[id.toInt()]?.decode(buffer)
//          when {
//            msg is ErrorResponse -> throw IOException("${msg.level}: ${msg.message} (${msg.code})")
//            msg is NotificationResponse -> channels[msg.channel]?.send(msg.payload)
//            msg != null -> send(msg)
//          }
//        }
//      }

      val responses = produce<Message> {
        val buffer = Buffer()

        val direct = ByteBuffer.allocateDirect(8192)
        while (socket.isOpen && isActive) {
          buffer.clear()
          direct.clear()
          direct.limit(5)
          socket.aRequire(direct, 5)
          direct.flip()

          val id = direct.get()
          var length = direct.getInt() - 4
          while (length > 0) {
            direct.clear()
            if (length < direct.capacity()) direct.limit(length)

            val read = socket.aRead(direct)
            if (read > 0) {
              length -= read
              direct.flip()
              buffer.write(direct)
            }
          }

          val msg = factories[id.toInt()]?.decode(buffer)
          println("received=$msg")
          when {
            msg is ErrorResponse -> throw IOException("${msg.level}: ${msg.message} (${msg.code})")
            msg is NotificationResponse -> channels[msg.channel]?.send(msg.payload)
            msg != null -> send(msg)
            else -> println("    unknown=${id.toChar()}")
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

private suspend fun AsynchronousSocketChannel.aRequire(
  direct: ByteBuffer,
  byteCount: Int
) {
  require(direct.remaining() >= byteCount)
  var remaining = byteCount
  while (remaining > 0) {
    val read = aRead(direct)
    if (read == -1) {
      throw EOFException()
    }
    remaining -= read
  }
}
