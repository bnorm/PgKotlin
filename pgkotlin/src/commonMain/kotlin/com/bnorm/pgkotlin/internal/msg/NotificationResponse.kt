package com.bnorm.pgkotlin.internal.msg

import kotlinx.io.core.*

/**
 * See [PostgreSQL message formats](https://www.postgresql.org/docs/current/static/protocol-message-formats.html)
 *
 * <pre>
 * NotificationResponse (B)
 *   Byte1('A')
 *     Identifies the message as a notification response.
 *   Int32
 *     Length of message contents in bytes, including self.
 *   Int32
 *     The process ID of the notifying backend process.
 *   String
 *     The name of the channel that the notify has been raised on.
 *   String
 *     The "payload" string passed from the notifying process.
 * </pre>
 */
internal data class NotificationResponse(
  val backend: Int,
  val channel: String,
  val payload: String
) : Message {
  override val id = Companion.id

  companion object : Message.Factory<NotificationResponse> {
    override val id = 'A'.toByte()
    override fun decode(source: Input): NotificationResponse {
      return NotificationResponse(
        source.readInt(),
        source.readUtf8Terminated(),
        source.readUtf8Terminated()
      )
    }
  }
}
