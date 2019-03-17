package com.bnorm.pgkotlin.internal

import com.bnorm.pgkotlin.PgClient
import com.bnorm.pgkotlin.Result
import com.bnorm.pgkotlin.Statement
import com.bnorm.pgkotlin.Transaction
import com.bnorm.pgkotlin.internal.msg.Message
import com.bnorm.pgkotlin.internal.msg.NotificationResponse
import com.bnorm.pgkotlin.internal.msg.Request
import com.bnorm.pgkotlin.internal.msg.factories
import com.bnorm.pgkotlin.internal.protocol.Postgres10
import com.bnorm.pgkotlin.internal.protocol.Protocol
import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.BroadcastChannel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.actor
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.isActive
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.suspendCancellableCoroutine
import okio.Buffer
import org.intellij.lang.annotations.Language
import java.net.InetSocketAddress
import java.net.SocketAddress
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousCloseException
import java.nio.channels.AsynchronousSocketChannel
import java.nio.channels.CompletionHandler
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

internal class Connection(
  private val protocol: Protocol,
  private val channels: MutableMap<String, BroadcastChannel<String>>,
  private val job: Job
) : PgClient, CoroutineScope {

  private val statementCount = AtomicInteger(0)

  override val coroutineContext: CoroutineContext
    get() = Dispatchers.Default + job

  override suspend fun prepare(sql: String, name: String?): Statement {
    val actualName = name ?: "statement_" + statementCount.getAndIncrement()
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

      val sessionFactory = object : PostgresSessionFactory {
        override suspend fun <R> session(block: suspend PostgresSession.() -> R): R {
          return block(ChannelPostgresSession(requests, responses))
        }
      }
      val protocol = Postgres10(sessionFactory, { pgEncode() }, scope)

      protocol.startup(username, password, database)

      return Connection(protocol, channels, job)
    }

    private fun CoroutineScope.sink(socket: AsynchronousSocketChannel) = actor<Request> {
      socket.use { socket ->
        val buffer = Buffer()
        val cursor = Buffer.UnsafeCursor()

        for (msg in channel) {
          msg.writeTo(buffer)
          debug { println("sending=$msg") }
          socket.aWrite(buffer, buffer.size, cursor)
        }
      }
    }

    private fun CoroutineScope.source(
      socket: AsynchronousSocketChannel,
      channels: MutableMap<String, BroadcastChannel<String>>
    ) = produce<Message> {
      val buffer = Buffer()
      val cursor = Buffer.UnsafeCursor()

      while (socket.isOpen && isActive) {
        while (buffer.size < 5) socket.aRead(buffer, cursor)

        val id = buffer.readByte()
        val length = (buffer.readInt() - 4).toLong()
        while (buffer.size < length) socket.aRead(buffer, cursor)

        val msg = factories[id.toInt()]?.decode(buffer)
        debug { println("received=$msg") }
        when {
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
    val oldSize = buffer.size
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

private suspend fun AsynchronousSocketChannel.aWrite(
  buffer: Buffer,
  byteCount: Long,
  cursor: Buffer.UnsafeCursor
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

// Pulled from the old JDK NIO coroutine integration module

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

