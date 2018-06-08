package com.bnorm.pgkotlin.internal.msg

import com.bnorm.pgkotlin.internal.okio.BufferedSink
import com.bnorm.pgkotlin.internal.okio.Buffer
import com.bnorm.pgkotlin.internal.okio.ByteString
import com.bnorm.pgkotlin.internal.okio.encodeUtf8

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
    sink.write(hash ?: encodeUtf8(password))
    sink.writeByte(0)
  }

  companion object {
    fun create(username: String, password: String, salt: ByteString?): PasswordMessage {
      if (salt == null) return PasswordMessage(password, null)

      val intermediate = Buffer().apply {
        writeUtf8(username)
        writeUtf8(password)
      }.readByteString().md5().hex()

      val md5 = Buffer().apply {
        writeUtf8(intermediate)
        write(salt)
      }.readByteString().md5().hex()

      return PasswordMessage(
        password,
        encodeUtf8("md5$md5")
      )
    }
  }
}
