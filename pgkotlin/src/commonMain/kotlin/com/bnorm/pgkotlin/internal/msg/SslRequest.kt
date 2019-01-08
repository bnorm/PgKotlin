package com.bnorm.pgkotlin.internal.msg

import kotlinx.io.core.*

/**
 * See [PostgreSQL message formats](https://www.postgresql.org/docs/current/static/protocol-message-formats.html)
 *
 * <pre>
 * SSLRequest (F)
 *   Int32(8)
 *     Length of message contents in bytes, including self.
 *   Int32(80877103)
 *     The SSL request code. The value is chosen to contain 1234 in the most significant 16 bits, and 5679 in the least significant 16 bits. (To avoid confusion, this code must not be the same as any protocol version number.)
 * </pre>
 */
internal object SslRequest : Request {
  override val id: Byte = -1
  override fun encode(sink: Output) {
    sink.writeInt(80877103)
  }

  override fun writeTo(sink: Output) {
    sink.writeByte(id)
    sink.writeInt(8)
    encode(sink)
  }

  override fun toString() = "SslRequest"
}
