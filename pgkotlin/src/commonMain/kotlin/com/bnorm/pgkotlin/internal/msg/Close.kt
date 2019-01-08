package com.bnorm.pgkotlin.internal.msg

import kotlinx.io.core.*

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
internal data class Close(
  private val type: StatementType,
  private val name: String = ""
) : Request {
  override val id = 'C'.toByte()
  override fun encode(sink: Output) {
    sink.writeByte(type.code)
    sink.writeUtf8Terminated(name)
  }
}
