package com.bnorm.pgkotlin.internal.msg

import okio.BufferedSink

/**
 * See [PostgreSQL message formats](https://www.postgresql.org/docs/current/static/protocol-message-formats.html)
 *
 * <pre>
 * CancelRequest (F)
 *   Int32(16)
 *     Length of message contents in bytes, including self.
 *   Int32(80877102)
 *     The cancel request code. The value is chosen to contain 1234 in the most significant 16 bits, and 5678 in the least significant 16 bits. (To avoid confusion, this code must not be the same as any protocol version number.)
 *   Int32
 *     The process ID of the target backend.
 *   Int32
 *     The secret key for the target backend.
 * </pre>
 */
internal data class CancelRequest(
  private val processId: Int,
  private val secretKey: Int
) : Request {
  override val id: Int = -1
  override fun encode(sink: BufferedSink) {
    sink.writeInt(80877102) // cancel request code
    sink.writeInt(processId)
    sink.writeInt(secretKey)
  }
}
