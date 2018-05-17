package com.bnorm.pgkotlin.internal.msg

import okio.BufferedSink
import okio.ByteString
import okio.HashingSink
import okio.Okio

/**
 * See [PostgreSQL message formats](https://www.postgresql.org/docs/current/static/protocol-message-formats.html)
 *
 * <pre>
 * PasswordMessage (F)
 *   Byte1('p')
 *     Identifies the message as a password response. Note that this is also used for GSSAPI and SSPI response messages (which is really a design error, since the contained data is not a null-terminated string in that case, but can be arbitrary binary data).
 *   Int32
 *     Length of message contents in bytes, including self.
 *   String
 *     The password (encrypted, if requested).
 * </pre>
 */
internal data class PasswordMessage private constructor(
  private val password: String,
  private val hash: ByteString?
) : Request {
  override val id: Int = 'p'.toInt()
  override fun encode(sink: BufferedSink) {
    sink.write(hash ?: ByteString.encodeUtf8(password))
    sink.writeByte(0)
  }

  companion object {
    fun create(username: String, password: String, salt: ByteString?): PasswordMessage {
      if (salt == null) return PasswordMessage(password, null)

      val md5 = HashingSink.md5(Okio.blackhole())
      Okio.buffer(md5).use { sink ->
        sink.writeUtf8(username)
        sink.writeUtf8(password)
      }
      val intermediate = md5.hash().hex()
      Okio.buffer(md5).use { sink ->
        sink.writeUtf8(intermediate)
        sink.write(salt)
      }
      val hash = ByteString.encodeUtf8("md5" + md5.hash().hex())
      return PasswordMessage(password, hash)
    }
  }
}
