package com.bnorm.pgkotlin

import com.bnorm.pgkotlin.internal.protocol.Protocol

class Statement internal constructor(
  internal val name: String,
  private val protocol: Protocol
) {
  init {
    require(name.isNotEmpty()) { "Cannot use unnamed prepared statement" }
  }

  suspend fun query(vararg params: Any?): Result? {
    return protocol.execute(this, params.toList())
  }

  suspend fun close() {
    protocol.closeStatement(this)
  }
}
