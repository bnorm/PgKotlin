package com.bnorm.pgkotlin.internal.msg

import okio.BufferedSource

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
  val process: Int,
  val secret: Int
) : Message {
  override val id: Int = Companion.id

  companion object : Message.Factory<BackendKeyData> {
    override val id: Int = 'K'.toInt()
    override fun decode(source: BufferedSource): BackendKeyData {
      val process = source.readInt()
      val secret = source.readInt()
      return BackendKeyData(process, secret)
    }
  }
}
