package com.bnorm.pgkotlin.internal

import com.bnorm.pgkotlin.*
import com.bnorm.pgkotlin.internal.msg.*
import com.bnorm.pgkotlin.internal.protocol.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.io.*
import kotlinx.io.core.*
import org.intellij.lang.annotations.*
import java.io.*
import java.net.*
import java.nio.*
import java.nio.channels.*
import java.nio.channels.CompletionHandler
import java.util.concurrent.*
import java.util.concurrent.atomic.*
import kotlin.coroutines.*

internal class Connection(
  private val protocol: Protocol,
  private val channels: MutableMap<String, BroadcastChannel<String>>,
  private val job: Job
) : PgClient, CoroutineScope {

  private val statementCount = AtomicInteger(0)

  override val coroutineContext: CoroutineContext
    get() = Dispatchers.Default + job

  override suspend fun prepare(sql: String, name: String?): Statement {
    val actualName = name ?: "statement_"+statementCount.getAndIncrement()
    return protocol.createStatement(sql, actualName)
  }

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
    return protocol.beginTransaction(this)
  }

  override suspend fun query(
    @Language("PostgreSQL") sql: String,
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
      port: Int = 5432,
      database: String = "postgres",
      username: String = "postgres",
      password: String? = null
    ) = connect(InetSocketAddress(hostname, port), database, username, password)

    suspend fun connect(
      address: InetSocketAddress = InetSocketAddress("localhost", 5432),
      database: String = "postgres",
      username: String = "postgres",
      password: String? = null
    ): Connection {
      val job = Job()
      val scope = object : CoroutineScope {
        override val coroutineContext: CoroutineContext
          get() = Dispatchers.Default + job
      }

      val socket = AsynchronousSocketChannel.open()
      socket.aConnect(address)

      val requests = scope.sink(socket)
      val channels = mutableMapOf<String, BroadcastChannel<String>>()
      val responses = scope.source(socket, channels)

      val protocol = Postgres10(requests, responses, { pgEncode() }, scope)

      protocol.startup(username, password, database)

      return Connection(protocol, channels, job)
    }

    private fun CoroutineScope.sink(socket: AsynchronousSocketChannel) = actor<Request> {
      socket.use { socket ->
        for (msg in this@actor.channel) {
          val packet = buildPacket {
            msg.writeTo(this)
          }
          debug { println("sending=$msg") }
          packet.readDirect(packet.remaining.toInt()) {
            socket.aWrite(it)
          }
        }
      }
    }

    private fun CoroutineScope.source(
      socket: AsynchronousSocketChannel,
      channels: MutableMap<String, BroadcastChannel<String>>
    ): ReceiveChannel<Message> {
      val source = writer(Dispatchers.IO) {
        // kotlinx.io defines a default buffer size of 4096
        // if something larger is configured, use that value instead, as we want the max throughput?
        // val size = (System.getProperty("kotlinx.io.buffer.size")?.toIntOrNull() ?: 4096) - 8

        while (socket.isOpen && isActive) {
          channel.writePacket { writeDirect(1) { socket.aRead(it) } }
          channel.flush()
        }
      }
      return produce<Message> {
        while (isActive) {
          with(source) {
            val id = channel.readByte()
            val length = (channel.readInt() - 4)
            val packet = channel.readPacket(length)
            debug { println("received(id=$id length=$length)") }

            val msg = factories[id]?.decode(packet)
            debug { println("received(msg=$msg, remaining=${packet.remaining})") }
            when {
              msg is ErrorResponse -> throw IOException("${msg.level}: ${msg.message} (${msg.code})")
              msg is NotificationResponse -> channels[msg.channel]?.send(msg.payload)
              msg != null -> send(msg)
              else -> {
                println("unknown(id=$id length=$length)")
                packet.release()
              }
            }
          }
        }
      }
    }
  }
}

// Pulled from the old JDK NIO coroutine integration module
// TODO(bnorm): would it be faster to use our own selector?

/**
 * Performs [AsynchronousSocketChannel.connect] without blocking a thread and resumes when asynchronous operation completes.
 * This suspending function is cancellable.
 * If the [Job] of the current coroutine is cancelled or completed while this suspending function is waiting, this function
 * *closes the underlying channel* and immediately resumes with [CancellationException].
 */
suspend fun AsynchronousSocketChannel.aConnect(
  socketAddress: SocketAddress
) = suspendCancellableCoroutine<Unit> { cont ->
  connect(socketAddress, cont, AsyncVoidIOHandler)
  closeOnCancel(cont)
}

/**
 * Performs [AsynchronousSocketChannel.read] without blocking a thread and resumes when asynchronous operation completes.
 * This suspending function is cancellable.
 * If the [Job] of the current coroutine is cancelled or completed while this suspending function is waiting, this function
 * *closes the underlying channel* and immediately resumes with [CancellationException].
 */
suspend fun AsynchronousSocketChannel.aRead(
  buf: ByteBuffer,
  timeout: Long = 0L,
  timeUnit: TimeUnit = TimeUnit.MILLISECONDS
) = suspendCancellableCoroutine<Int> { cont ->
  read(buf, timeout, timeUnit, cont, asyncIOHandler())
  closeOnCancel(cont)
}

/**
 * Performs [AsynchronousSocketChannel.write] without blocking a thread and resumes when asynchronous operation completes.
 * This suspending function is cancellable.
 * If the [Job] of the current coroutine is cancelled or completed while this suspending function is waiting, this function
 * *closes the underlying channel* and immediately resumes with [CancellationException].
 */
suspend fun AsynchronousSocketChannel.aWrite(
  buf: ByteBuffer,
  timeout: Long = 0L,
  timeUnit: TimeUnit = TimeUnit.MILLISECONDS
) = suspendCancellableCoroutine<Int> { cont ->
  write(buf, timeout, timeUnit, cont, asyncIOHandler())
  closeOnCancel(cont)
}

// ---------------- private details ----------------

private fun java.nio.channels.Channel.closeOnCancel(cont: CancellableContinuation<*>) {
  cont.invokeOnCancellation {
    try {
      close()
    } catch (ex: Throwable) {
      // Specification says that it is Ok to call it any time, but reality is different,
      // so we have just to ignore exception
    }
  }
}

@Suppress("UNCHECKED_CAST")
private fun <T> asyncIOHandler(): java.nio.channels.CompletionHandler<T, CancellableContinuation<T>> =
  AsyncIOHandlerAny as java.nio.channels.CompletionHandler<T, CancellableContinuation<T>>

private object AsyncIOHandlerAny : CompletionHandler<Any, CancellableContinuation<Any>> {
  override fun completed(result: Any, cont: CancellableContinuation<Any>) {
    cont.resume(result)
  }

  override fun failed(ex: Throwable, cont: CancellableContinuation<Any>) {
    // just return if already cancelled and got an expected exception for that case
    if (ex is AsynchronousCloseException && cont.isCancelled) return
    cont.resumeWithException(ex)
  }
}

private object AsyncVoidIOHandler : CompletionHandler<Void?, CancellableContinuation<Unit>> {
  override fun completed(result: Void?, cont: CancellableContinuation<Unit>) {
    cont.resume(Unit)
  }

  override fun failed(ex: Throwable, cont: CancellableContinuation<Unit>) {
    // just return if already cancelled and got an expected exception for that case
    if (ex is AsynchronousCloseException && cont.isCancelled) return
    cont.resumeWithException(ex)
  }
}

