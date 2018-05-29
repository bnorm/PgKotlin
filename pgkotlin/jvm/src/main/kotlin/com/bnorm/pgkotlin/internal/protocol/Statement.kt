package com.bnorm.pgkotlin.internal.protocol

abstract class Statement(val name: String) {
  init {
    require(name.isNotEmpty()) { "Cannot use unnamed prepared statement" }
  }

  abstract suspend fun close()
}
