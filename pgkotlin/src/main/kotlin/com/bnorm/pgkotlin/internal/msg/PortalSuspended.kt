package com.bnorm.pgkotlin.internal.msg

import com.bnorm.pgkotlin.internal.okio.BufferedSource

/**
 * See [PostgreSQL message formats](https://www.postgresql.org/docs/current/static/protocol-message-formats.html)
 *
 * <pre>
 * PortalSuspended (B)
 *   Byte1('s')
 *     Identifies the message as a portal-suspended indicator. Note this only appears if an Execute message's row-count limit was reached.
 *   Int32(4)
 *     Length of message contents in bytes, including self.
 * </pre>
 */
internal object PortalSuspended : Message, Message.Factory<PortalSuspended> {
  override val id: Int = 's'.toInt()
  override fun decode(source: BufferedSource) = this
  override fun toString() = "PortalSuspended"
}
