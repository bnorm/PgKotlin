package com.bnorm.pgkotlin.internal.msg

import okio.BufferedSink

/**
 * See [PostgreSQL message formats](https://www.postgresql.org/docs/current/static/protocol-message-formats.html)
 *
 * <pre>
 * Describe (F)
 *   Byte1('D')
 *     Identifies the message as a Describe command.
 *   Int32
 *     Length of message contents in bytes, including self.
 *   Byte1
 *     'S' to describe a prepared statement; or 'P' to describe a portal.
 *   String
 *     The name of the prepared statement or portal to describe (an empty string selects the unnamed prepared statement or portal).
 * </pre>
 */
internal object Describe : Request {
  override val id: Int = 'D'.toInt()
  override fun encode(sink: BufferedSink) {
    sink.writeByte('S'.toInt())
    sink.writeByte(0)
  }
}
