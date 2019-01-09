package com.bnorm.pgkotlin.internal

import kotlinx.cinterop.*
import kotlinx.coroutines.*
import platform.posix.*
import kotlin.coroutines.*


sealed class WaitingFor {
  class Read(val data: CArrayPointer<ByteVar>,
             val length: ULong,
             val continuation: Continuation<ULong>) : WaitingFor()

  class Write(val data: CArrayPointer<ByteVar>,
              val length: ULong,
              val continuation: Continuation<Unit>) : WaitingFor()
}

class Client(val clientFd: Int, val waitingList: MutableMap<Int, WaitingFor>) {
  suspend fun read(data: CArrayPointer<ByteVar>, dataLength: ULong): ULong {
    val length = read(clientFd, data, dataLength)
    if (length >= 0)
      return length.toULong()
    if (posix_errno() != EWOULDBLOCK)
      throw IOException(getUnixError())
    // Save continuation and suspend.
    return suspendCoroutine { continuation ->
      waitingList.put(clientFd, WaitingFor.Read(data, dataLength, continuation))
    }
  }

  suspend fun write(data: CArrayPointer<ByteVar>, length: ULong) {
    val written = write(clientFd, data, length)
    if (written >= 0)
      return
    if (posix_errno() != EWOULDBLOCK)
      throw IOException(getUnixError())
    // Save continuation and suspend.
    return suspendCoroutine { continuation ->
      waitingList.put(clientFd, WaitingFor.Write(data, length, continuation))
    }
  }
}

open class EmptyContinuation(override val context: CoroutineContext = EmptyCoroutineContext) : Continuation<Any?> {
  companion object : EmptyContinuation()

  override fun resumeWith(result: Result<Any?>) {
    result.getOrThrow()
  }
}

suspend fun clientLoop(fd: Int, block: suspend Client.() -> Unit) {
  memScoped {
    val waitingList = mutableMapOf<Int, WaitingFor>()

    val tv = alloc<timespec>()
    tv.tv_sec = 0
    tv.tv_nsec = 50_000_000

    val readfds = alloc<fd_set>()
    val writefds = alloc<fd_set>()
    val errorfds = alloc<fd_set>()

    println("Starting client loop")
    block.startCoroutine(Client(fd, waitingList), EmptyContinuation)

    while (true) {

      posix_FD_ZERO(readfds.ptr)
      posix_FD_ZERO(writefds.ptr)
      posix_FD_ZERO(errorfds.ptr)
      for ((socketFd, watingFor) in waitingList) {
        when (watingFor) {
          is WaitingFor.Read -> posix_FD_SET(socketFd, readfds.ptr)
          is WaitingFor.Write -> posix_FD_SET(socketFd, writefds.ptr)
        }
        posix_FD_SET(socketFd, errorfds.ptr)
      }

      println("select")
      delay(1)
      pselect(fd + 1, readfds.ptr, writefds.ptr, errorfds.ptr, tv.ptr, null)
        .ensureUnixCallResult { it >= 0 }

      loop@ for (socketFd in 0..fd) {
        val waitingFor = waitingList[socketFd]
        val errorOccured = posix_FD_ISSET(socketFd, errorfds.ptr) != 0
        if (posix_FD_ISSET(socketFd, readfds.ptr) != 0
          || posix_FD_ISSET(socketFd, writefds.ptr) != 0
          || errorOccured) {
          when (waitingFor) {
            is WaitingFor.Read -> {
              if (errorOccured)
                waitingFor.continuation.resumeWithException(IOException("Connection was closed by peer"))

              // Resume reading operation.
              waitingList.remove(socketFd)
              val length = read(socketFd, waitingFor.data, waitingFor.length)
              if (length < 0) // Read error.
                waitingFor.continuation.resumeWithException(IOException(getUnixError()))
              waitingFor.continuation.resume(length.toULong())
            }
            is WaitingFor.Write -> {
              if (errorOccured)
                waitingFor.continuation.resumeWithException(IOException("Connection was closed by peer"))

              // Resume writing operation.
              waitingList.remove(socketFd)
              val written = write(socketFd, waitingFor.data, waitingFor.length)
              if (written < 0) // Write error.
                waitingFor.continuation.resumeWithException(IOException(getUnixError()))
              waitingFor.continuation.resume(Unit)
            }
          }
        }
      }
    }
  }
}

class IOException(message: String) : RuntimeException(message)

fun getUnixError() = strerror(posix_errno())!!.toKString()

inline fun Int.ensureUnixCallResult(predicate: (Int) -> Boolean): Int {
  if (!predicate(this)) {
    throw Error(getUnixError())
  }
  return this
}

inline fun Long.ensureUnixCallResult(predicate: (Long) -> Boolean): Long {
  if (!predicate(this)) {
    throw Error(getUnixError())
  }
  return this
}

inline fun ULong.ensureUnixCallResult(predicate: (ULong) -> Boolean): ULong {
  if (!predicate(this)) {
    throw Error(getUnixError())
  }
  return this
}

internal fun Int.isMinusOne() = (this == -1)
private fun Long.isMinusOne() = (this == -1L)
private fun ULong.isMinusOne() = (this == ULong.MAX_VALUE)
