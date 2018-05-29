package com.bnorm.pgkotlin.internal

internal class PgProtocolException(msg: String = "", throwable: Throwable? = null) :
  RuntimeException(msg, throwable)
