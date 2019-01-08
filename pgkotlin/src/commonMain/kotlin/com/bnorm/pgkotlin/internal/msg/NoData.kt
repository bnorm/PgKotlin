package com.bnorm.pgkotlin.internal.msg

import kotlinx.io.core.*

/**
 * See [PostgreSQL message formats](https://www.postgresql.org/docs/current/static/protocol-message-formats.html)
 *
 * <pre>
 * NoData (B)
 *   Byte1('n')
 *     Identifies the message as a no-data indicator.
 *   Int32(4)
 *     Length of message contents in bytes, including self.
 * </pre>
 */
internal object NoData : Message, Message.Factory<NoData> {
  override val id = 'n'.toByte()
  override fun decode(source: Input) = this
  override fun toString() = "NoData"
}
