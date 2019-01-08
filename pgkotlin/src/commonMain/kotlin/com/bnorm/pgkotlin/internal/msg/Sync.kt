package com.bnorm.pgkotlin.internal.msg

import kotlinx.io.core.*

/**
 * See [PostgreSQL message formats](https://www.postgresql.org/docs/current/static/protocol-message-formats.html)
 *
 * <pre>
 * Sync (F)
 *   Byte1('S')
 *     Identifies the message as a Sync command.
 *   Int32(4)
 *     Length of message contents in bytes, including self.
 * </pre>
 */
internal object Sync : Request {
  override val id = 'S'.toByte()
  override fun encode(sink: Output) {}

  override fun writeTo(sink: Output) {
    sink.writeByte(id)
    sink.writeInt(4)
  }

  override fun toString() = "Sync"
}
