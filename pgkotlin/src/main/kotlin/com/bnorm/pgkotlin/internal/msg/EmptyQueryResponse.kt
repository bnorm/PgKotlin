package com.bnorm.pgkotlin.internal.msg

import okio.BufferedSource

/**
 * See [PostgreSQL message formats](https://www.postgresql.org/docs/current/static/protocol-message-formats.html)
 *
 * <pre>
 * EmptyQueryResponse (B)
 *   Byte1('I')
 *     Identifies the message as a response to an empty query string. (This substitutes for CommandComplete.)
 *   Int32(4)
 *     Length of message contents in bytes, including self.
 * </pre>
 */
internal object EmptyQueryResponse : Message, Message.Factory<EmptyQueryResponse> {
  override val id: Int = 'I'.toInt()
  override fun decode(source: BufferedSource): EmptyQueryResponse {
    return EmptyQueryResponse
  }

  override fun toString(): String {
    return "EmptyQueryResponse"
  }
}
