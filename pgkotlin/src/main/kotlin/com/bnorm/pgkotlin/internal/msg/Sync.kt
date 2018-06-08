package com.bnorm.pgkotlin.internal.msg

import com.bnorm.pgkotlin.internal.okio.BufferedSink

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
  override val id: Int = 'S'.toInt()
  override fun encode(sink: BufferedSink) {}

  override fun toString() = "Sync"
}
