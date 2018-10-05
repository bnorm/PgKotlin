package com.bnorm.pgkotlin.internal.msg

import com.bnorm.pgkotlin.internal.okio.BufferedSink

/**
 * See [PostgreSQL message formats](https://www.postgresql.org/docs/current/static/protocol-message-formats.html)
 *
 * <pre>
 * Query (F)
 *   Byte1('Q')
 *     Identifies the message as a simple query.
 *   Int32
 *     Length of message contents in bytes, including self.
 *   String
 *     The queryRows string itself.
 * </pre>
 */
internal data class Query(
  private val sql: String
) : Request {
  override val id: Int = 'Q'.toInt()
  override fun encode(sink: BufferedSink) {
    sink.writeUtf8Terminated(sql)
  }
}
