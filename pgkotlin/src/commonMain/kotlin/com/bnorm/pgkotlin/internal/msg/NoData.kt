package com.bnorm.pgkotlin.internal.msg

import com.bnorm.pgkotlin.internal.okio.BufferedSource

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
  override val id: Int = 'n'.toInt()
  override fun decode(source: BufferedSource) = this
  override fun toString() = "NoData"
}
