package com.bnorm.pgkotlin.internal

class PgProtocolException(msg: String = "", throwable: Throwable? = null) :
  RuntimeException(msg, throwable)

fun <R> pgProtocol(block: () -> R): R {
  try {
    return block()
  } catch (t: Throwable) {
    if (t is PgProtocolException) throw t
    else throw PgProtocolException(throwable = t)
  }
}