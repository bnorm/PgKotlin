package com.bnorm.pgkotlin.internal.msg

import okio.BufferedSink

/**
 * See [PostgreSQL message formats](https://www.postgresql.org/docs/current/static/protocol-message-formats.html)
 *
 * <pre>
 * Close (F)
 *   Byte1('C')
 *     Identifies the message as a Close command.
 *   Int32
 *     Length of message contents in bytes, including self.
 *   Byte1
 *     'S' to close a prepared statement; or 'P' to close a portal.
 *   String
 *     The name of the prepared statement or portal to close (an empty string selects the unnamed prepared statement or portal).
 * </pre>
 */
internal object Close : Request {
  override val id: Int = 'C'.toInt()
  override fun encode(sink: BufferedSink) {
    sink.writeByte('S'.toInt())
    sink.writeByte(0)
  }
}
