package com.bnorm.pgkotlin.internal.msg

import com.bnorm.pgkotlin.internal.okio.BufferedSource

/**
 * See [PostgreSQL message formats](https://www.postgresql.org/docs/current/static/protocol-message-formats.html)
 *
 * <pre>
 * ParseComplete (B)
 *   Byte1('1')
 *     Identifies the message as a Parse-complete indicator.
 *   Int32(4)
 *     Length of message contents in bytes, including self.
 * </pre>
 */
internal object ParseComplete : Message, Message.Factory<ParseComplete> {
  override val id: Int = '1'.toInt()
  override fun decode(source: BufferedSource) = this
  override fun toString() = "ParseComplete"
}
