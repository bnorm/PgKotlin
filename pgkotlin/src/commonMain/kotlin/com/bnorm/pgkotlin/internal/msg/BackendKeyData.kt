package com.bnorm.pgkotlin.internal.msg

import kotlinx.io.core.*

/**
 * See [PostgreSQL message formats](https://www.postgresql.org/docs/current/static/protocol-message-formats.html)
 *
 * <pre>
 * BackendKeyData (B)
 *   Byte1('K')
 *     Identifies the message as cancellation key data. The frontend must save these values if it wishes to be able to issue CancelRequest messages later.
 *   Int32(12)
 *     Length of message contents in bytes, including self.
 *   Int32
 *     The process ID of this backend.
 *   Int32
 *     The secret key of this backend.
 * </pre>
 */
internal data class BackendKeyData(
  val processId: Int,
  val secretKey: Int
) : Message {
  override val id = Companion.id

  companion object : Message.Factory<BackendKeyData> {
    override val id = 'K'.toByte()
    override fun decode(source: Input): BackendKeyData {
      val processId = source.readInt()
      val secretKey = source.readInt()
      return BackendKeyData(processId, secretKey)
    }
  }
}
