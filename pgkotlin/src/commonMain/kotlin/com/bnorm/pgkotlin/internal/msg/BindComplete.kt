package com.bnorm.pgkotlin.internal.msg

import com.bnorm.pgkotlin.internal.okio.BufferedSource

/**
 * See [PostgreSQL message formats](https://www.postgresql.org/docs/current/static/protocol-message-formats.html)
 *
 * <pre>
 * BindComplete (B)
 *   Byte1('2')
 *     Identifies the message as a Bind-complete indicator.
 *   Int32(4)
 *     Length of message contents in bytes, including self.
 * </pre>
 */
internal object BindComplete : Message, Message.Factory<BindComplete> {
  override val id: Int = '2'.toInt()
  override fun decode(source: BufferedSource) = this
  override fun toString() = "BindComplete"
}
