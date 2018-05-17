package com.bnorm.pgkotlin.internal.msg

import okio.BufferedSource

/**
 * See [PostgreSQL message formats](https://www.postgresql.org/docs/current/static/protocol-message-formats.html)
 *
 * <pre>
 * ErrorResponse (B)
 *  Byte1('E')
 *      Identifies the message as an error.
 *  Int32
 *      Length of message contents in bytes, including self.
 *  The message body consists of one or more identified fields, followed by a zero byte as a terminator. Fields can appear in any order. For each field there is the following:
 *  Byte1
 *      A code identifying the field type; if zero, this is the message terminator and no string follows. The presently defined field types are listed in Section 46.6. Since more field types might be added in future, frontends should silently ignore fields of unrecognized type.
 *  String
 *      The field value.
 * </pre>
 */
internal data class ErrorResponse(
  val level: Level,
  val code: String,
  val message: String
) : Message {
  override val id: Int = Companion.id

  enum class Level {
    ERROR, FATAL, PANIC, WARNING, NOTICE, DEBUG, INFO, LOG
  }

  companion object : Message.Factory<ErrorResponse> {
    override val id: Int = 'E'.toInt()
    override fun decode(source: BufferedSource): ErrorResponse {
      var level: Level? = null
      var code: String? = null
      var message: String? = null

      var type = source.readByte()
      while (type != 0.toByte()) {
        val value = source.readTerminatedString()
        when (type) {
          'S'.toByte() -> level = Level.valueOf(value)
          'C'.toByte() -> code = value
          'M'.toByte() -> message = value
        }
        type = source.readByte()
      }

      if (level == null || code == null || message == null)
        throw IllegalStateException()

      return ErrorResponse(level, code, message)
    }
  }
}
