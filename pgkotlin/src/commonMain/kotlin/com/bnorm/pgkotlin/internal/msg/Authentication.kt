package com.bnorm.pgkotlin.internal.msg

import com.bnorm.pgkotlin.internal.okio.BufferedSource
import com.bnorm.pgkotlin.internal.okio.ByteString

/**
 * See [PostgreSQL message formats](https://www.postgresql.org/docs/current/static/protocol-message-formats.html)
 *
 * <pre>
 * AuthenticationOk (B)
 *   Byte1('R')
 *     Identifies the message as an authentication request.
 *   Int32(8)
 *     Length of message contents in bytes, including self.
 *   Int32(0)
 *     Specifies that the authentication was successful.
 *
 * AuthenticationCleartextPassword (B)
 *   Byte1('R')
 *     Identifies the message as an authentication request.
 *   Int32(8)
 *     Length of message contents in bytes, including self.
 *   Int32(3)
 *     Specifies that a clear-text password is required.
 *
 * AuthenticationMD5Password (B)
 *   Byte1('R')
 *     Identifies the message as an authentication request.
 *   Int32(12)
 *     Length of message contents in bytes, including self.
 *   Int32(5)
 *     Specifies that an MD5-encrypted password is required.
 *   Byte4
 *     The salt to use when encrypting the password.
 * </pre>
 */
internal data class Authentication(
  val success: Boolean,
  val md5salt: ByteString?
) : Message {
  override val id: Int = Companion.id

  companion object : Message.Factory<Authentication> {
    private const val OK = 0
    private const val CLEARTEXT_PASSWORD = 3
    private const val PASSWORD_MD5_CHALLENGE = 5

    override val id: Int = 'R'.toInt()
    override fun decode(source: BufferedSource) = when (source.readInt()) {
      OK -> Authentication(true, null)
      CLEARTEXT_PASSWORD -> Authentication(false, null)
      PASSWORD_MD5_CHALLENGE -> Authentication(false, source.readByteString(4))
      else -> TODO()
    }
  }
}
