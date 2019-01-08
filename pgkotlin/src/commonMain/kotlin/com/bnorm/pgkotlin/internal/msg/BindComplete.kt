package com.bnorm.pgkotlin.internal.msg

import kotlinx.io.core.*

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
  override val id = '2'.toByte()
  override fun decode(source: Input) = this
  override fun toString() = "BindComplete"
}
