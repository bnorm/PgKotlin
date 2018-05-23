package com.bnorm.pgkotlin.internal.msg

import okio.BufferedSink

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
  override val id: Int = 'H'.toInt()
  override fun encode(sink: BufferedSink) {}

  override fun toString() = "Flush"
}
