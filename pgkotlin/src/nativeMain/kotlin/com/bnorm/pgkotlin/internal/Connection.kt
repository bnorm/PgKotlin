package com.bnorm.pgkotlin.internal

import com.bnorm.pgkotlin.*
import com.bnorm.pgkotlin.internal.msg.*
import com.bnorm.pgkotlin.internal.protocol.*
import kotlinx.cinterop.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.io.*
import kotlinx.io.core.*
import platform.posix.*
import kotlin.coroutines.*

internal class Connection(
  private val protocol: Protocol,
  private val channels: MutableMap<String, BroadcastChannel<String>>,
  private val job: Job
) : PgClient, CoroutineScope {

  private var statementCount: Int = 0

  override val coroutineContext: CoroutineContext
    get() = Dispatchers.Default + job

  override suspend fun prepare(sql: String, name: String?): Statement {
    val actualName = name ?: "statement_"+(statementCount++)
    return protocol.createStatement(sql, actualName)
  }

  override suspend fun listen(channel: String): BroadcastChannel<String> {
    val broadcast = channels.getOrPut(channel) {
      val delegate = BroadcastChannel<String>(Channel.CONFLATED)
      return@getOrPut object : BroadcastChannel<String> by delegate {
        override fun close(cause: Throwable?): Boolean {
          channels.remove(channel)
          var actual = cause
          try {
            runBlocking {
              query("UNLISTEN $channel")
            }
          } catch (t: Throwable) {
//            when (actual) {
//              null -> actual = t
//              else -> actual.(t)
//            }
          }
          return delegate.close(actual)
        }
      }
    }
    query("LISTEN $channel")
    return broadcast
  }

  override suspend fun begin(): Transaction {
    return protocol.beginTransaction(this)
  }

  override suspend fun query(
    sql: String,
    vararg params: Any?
  ): Result? {
    return if (params.isEmpty()) protocol.simpleQuery(sql)
    else protocol.extendedQuery(sql, params.toList())
  }

  override suspend fun close() {
    protocol.terminate()
    // TODO(bnorm): join?

    job.cancel()
  }

  companion object {
    suspend fun connect(
      hostname: String = "localhost",
      port: Short = 5432,
      database: String = "postgres",
      username: String = "postgres",
      password: String? = null
    ): Connection {
      println("Creating connection")

      val job = Job()
      val scope = object : CoroutineScope {
        override val coroutineContext: CoroutineContext
          get() = Dispatchers.Default + job
      }

      // TODO https://github.com/JetBrains/kotlin-native/blob/daf1a5fb8cdba2aea97d3a0cf53ad919fdb02b13/samples/nonBlockingEchoServer/src/nonBlockingEchoServerMain/kotlin/EchoServer.kt
      // TODO https://www.binarytides.com/code-a-simple-socket-client-class-in-c/

      val requests = Channel<Request>()
      val responses = Channel<Message>()

      job.invokeOnCompletion {
        requests.close()
        requests.close()
      }

      scope.launch(Dispatchers.Main) {
        memScoped {
          val serverAddr = alloc<sockaddr_in>()
          val listenFd = socket(AF_INET, SOCK_STREAM, 0)
            .ensureUnixCallResult { !it.isMinusOne() }

          with(serverAddr) {
            memset(this.ptr, 0, sockaddr_in.size.convert())
            sin_family = AF_INET.convert()
            sin_addr.s_addr = posix_htons(0).convert()
            sin_port = posix_htons(port).convert()
          }

          val tv = alloc<timeval>()
          tv.tv_sec = 0
          tv.tv_usec = 50_000

          setsockopt(listenFd, SOL_SOCKET, SO_RCVTIMEO, tv.ptr, timeval.size.convert())
          connect(listenFd, serverAddr.ptr.reinterpret(), sockaddr_in.size.toUInt())
            .ensureUnixCallResult { it == 0 }

          try {
            coroutineScope {
              listOf(
                launch(Dispatchers.Main) {
                  val source = ByteChannel()

                  launch {
                    // TODO(bnorm): this MUST suspend or the thread blocks
                    memScoped {
                      val bufferLength = 4088.toULong()
                      val buffer = allocArray<ByteVar>(bufferLength.toInt())
                      while (isActive) {
                        // kotlinx.io defines a default buffer size of 4096
                        // if something larger is configured, use that value instead, as we want the max throughput?
//                     val size = (System.getProperty("kotlinx.io.buffer.size")?.toIntOrNull() ?: 4096) - 8

//                        println("reading")
                        val length = read(listenFd, buffer, bufferLength)
//                        println("read: length=$length")
                        if (length > 0) {
                          source.writeAvailable(buffer, 0, length)
//                          println("flush")
                          source.flush()
                        } else {
                          delay(1)
                        }
                      }
                    }
                  }

                  while (isActive) {
                    val id = source.readByte()
                    val length = (source.readInt() - 4)
                    val packet = source.readPacket(length)

                    val msg = factories[id]?.decode(packet)
                    debug { println("received(msg=$msg)") }
                    when {
                      msg is ErrorResponse -> throw IOException("${msg.level}: ${msg.message} (${msg.code})")
                      msg != null -> responses.send(msg)
                      else -> {
                        println("unknown(id=$id length=$length)")
                        packet.release()
                      }
                    }
                  }
                },

                launch(Dispatchers.Main) {
                  val bufferLength = 4088.toULong()
                  val buffer = allocArray<ByteVar>(bufferLength.toInt())

                  for (msg in requests) {
                    val packet = buildPacket {
                      msg.writeTo(this)
                    }
                    debug { println("sending=$msg") }
                    packet.takeWhile {
                      val length = it.readAvailable(buffer, 0, bufferLength.toInt())
                      write(listenFd, buffer, length.toULong())
                      true
                    }
                  }
                })
            }.map { it.join() }
          } catch (e: IOException) {
            println("I/O error occurred: ${e.message}")
          }
        }
      }

      val channels = mutableMapOf<String, BroadcastChannel<String>>()
      val protocol = Postgres10(requests, responses, { pgEncode() }, scope)

      protocol.startup(username, password, database)

      return Connection(protocol, channels, job)
    }
  }
}
