package com.bnorm.pgkotlin.internal.msg

import okio.BufferedSource

/**
 * See [PostgreSQL message formats](https://www.postgresql.org/docs/current/static/protocol-message-formats.html)
 *
 * <pre>
 * CloseComplete (B)
 *   Byte1('3')
 *     Identifies the message as a Close-complete indicator.
 *   Int32(4)
 *     Length of message contents in bytes, including self.
 * </pre>
 */
internal object CloseComplete : Message, Message.Factory<CloseComplete> {
  override val id: Int = '3'.toInt()
  override fun decode(source: BufferedSource) = this

  override fun toString() = "CloseComplete"
}
