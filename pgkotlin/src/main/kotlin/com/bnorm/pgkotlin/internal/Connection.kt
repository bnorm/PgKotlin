package com.bnorm.pgkotlin.internal

import com.bnorm.pgkotlin.*
import com.bnorm.pgkotlin.internal.msg.*
import kotlinx.coroutines.experimental.channels.*
import kotlinx.coroutines.experimental.nio.aConnect
import kotlinx.coroutines.experimental.nio.aRead
import kotlinx.coroutines.experimental.nio.aWrite
import kotlinx.coroutines.experimental.runBlocking
import okio.Buffer
import org.intellij.lang.annotations.Language
import java.io.IOException
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousSocketChannel

private inline fun Throwable?.chain(block: () -> Unit): Throwable? {
  var cause = this
  try {
    block()
  } catch (t: Throwable) {
    when (cause) {
      null -> cause = t
      else -> cause.addSuppressed(t)
    }
  }
  return cause
}

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
          return delegate.close(cause.chain {
            runBlocking {
              query("UNLISTEN $channel")
            }
          })
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

  override suspend fun query(
    @Language("PostgreSQL") sql: String,
    vararg params: Any
  ): Results {
    if (params.isEmpty()) {
      requests.send(Query(sql))
    } else {
      requests.send(Parse(sql))
      requests.send(Bind(params.map { it::class.toPgType().encode(it) }))
      requests.send(Describe)
      requests.send(Execute)
      requests.send(Close)
      requests.send(Sync)
    }

    // TODO(bnorm): is there a way to stream this?
    val columns = mutableListOf<Column<*>>()
    val rows = mutableListOf<Row>()
    for (msg in responses) {
      if (msg is CommandComplete) {
        check(msg.rows == rows.size) { "expected=${msg.rows} actual=${rows.size}" }
        break
      }

      when (msg) {
        is RowDescription -> {
          for (column in msg.columns) {
            columns.add(Column(column.name, column.type))
          }
        }
        is DataRow -> {
          rows.add(Row(msg.values.zip(columns) { value, column ->
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

    responses.receive() as? ReadyForQuery ?: throw PgProtocolException()
    return Results(columns, rows)
  }

  companion object {
    private val factories = listOf<Message.Factory<*>>(
      Authentication,
      CommandComplete,
      DataRow,
      ErrorResponse,
      NotificationResponse,
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
          socket.aRead(direct)
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
            else -> println("unknown=${id.toChar()}")
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
