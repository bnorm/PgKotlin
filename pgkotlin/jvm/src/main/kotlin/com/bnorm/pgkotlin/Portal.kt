package com.bnorm.pgkotlin

import com.bnorm.pgkotlin.internal.protocol.Protocol

class Portal internal constructor(
  internal val name: String,
  private val protocol: Protocol
) {
  init {
    require(name.isNotEmpty()) { "Cannot use unnamed portal" }
  }

  suspend fun close() {
    protocol.closePortal(this)
  }
}
