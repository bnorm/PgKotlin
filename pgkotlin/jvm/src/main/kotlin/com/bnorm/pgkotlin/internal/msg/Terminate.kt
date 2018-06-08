package com.bnorm.pgkotlin.internal.msg

import okio.BufferedSink

/**
 * See [PostgreSQL message formats](https://www.postgresql.org/docs/current/static/protocol-message-formats.html)
 *
 * <pre>
 * Terminate (F)
 *   Byte1('X')
 *     Identifies the message as a termination.
 *   Int32(4)
 *     Length of message contents in bytes, including self.
 * </pre>
 */
internal object Terminate : Request {
  override val id: Int = 'X'.toInt()
  override fun encode(sink: BufferedSink) {}

  override fun toString() = "Terminate"
}
