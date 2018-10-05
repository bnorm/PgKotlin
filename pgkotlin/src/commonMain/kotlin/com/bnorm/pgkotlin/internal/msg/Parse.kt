package com.bnorm.pgkotlin.internal.msg

import com.bnorm.pgkotlin.internal.okio.BufferedSink

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
 *     The number of parameter data types specified (can be zero). Note that this is not an indication of the number of parameters that might appear in the query string, only the number that the frontend wants to pre-specify types for.
 *   Then, for each parameter, there is the following:
 *   Int32
 *     Specifies the object ID of the parameter data type. Placing a zero here is equivalent to leaving the type unspecified.
 * </pre>
 */
internal data class Parse(
  private val sql: String,
  private val preparedStatement: String = "",
  private val types: List<Int?> = emptyList()
) : Request {
  override val id: Int = 'P'.toInt()
  override fun encode(sink: BufferedSink) {
    sink.writeUtf8Terminated(preparedStatement)
    sink.writeUtf8Terminated(sql)
    sink.writeShort(types.size)
    for (type in types) {
      sink.writeInt(type ?: 0)
    }
  }
}
