package com.bnorm.pgkotlin.internal.msg

import kotlinx.io.core.*

/**
 * See [PostgreSQL message formats](https://www.postgresql.org/docs/current/static/protocol-message-formats.html)
 *
 * <pre>
 * Flush (F)
 *   Byte1('H')
 *     Identifies the message as a Flush command.
 *   Int32(4)
 *     Length of message contents in bytes, including self.
 * </pre>
 */
internal object Flush : Request {
  override val id = 'H'.toByte()
  override fun encode(sink: Output) {}

  override fun writeTo(sink: Output) {
    sink.writeByte(id)
    sink.writeInt(4)
  }

  override fun toString() = "Flush"
}
