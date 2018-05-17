package com.bnorm.pgkotlin.internal

class PgProtocolException(msg: String = "", throwable: Throwable? = null) :
  RuntimeException(msg, throwable)
