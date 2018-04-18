package com.bnorm.pgkotlin.internal.msg

import okio.BufferedSink
import okio.BufferedSource

/**
 * See [PostgreSQL message formats](https://www.postgresql.org/docs/current/static/protocol-message-formats.html)
 *
 * <pre>
 * Parse (F)
 *   Byte1('P')
 *     Identifies the message as a Parse command.
 *   Int32
 *     Length of message contents in bytes, including self.
 *   String
 *     The name of the destination prepared statement (an empty string selects the unnamed prepared statement).
 *   String
 *     The query string to be parsed.
 *   Int16
 *     The number of parameter data types specified (can be zero). Note that this is not an indication of the number of parameters that might appear in the query string, only the number that the frontend wants to prespecify types for.
 *   Then, for each parameter, there is the following:
 *   Int32
 *     Specifies the object ID of the parameter data type. Placing a zero here is equivalent to leaving the type unspecified.
 * </pre>
 */
internal data class Parse(
  val sql: String
) : Request {
  override val id: Int = 'P'.toInt()
  override fun encode(sink: BufferedSink) {
    sink.writeByte(0) // prepared statement
    sink.write(sql.toByteArray())
    sink.writeByte(0)
    sink.writeShort(0) // no parameter types
  }
}
