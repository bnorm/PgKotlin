package com.bnorm.pgkotlin.internal.msg

import com.bnorm.pgkotlin.internal.okio.BufferedSource

/**
 * See [PostgreSQL message formats](https://www.postgresql.org/docs/current/static/protocol-message-formats.html)
 *
 * <pre>
 * ParameterStatus (B)
 *   Byte1('S')
 *     Identifies the message as a run-time parameter status report.
 *   Int32
 *     Length of message contents in bytes, including self.
 *   String
 *     The name of the run-time parameter being reported.
 *   String
 *     The current value of the parameter.
 * </pre>
 */
internal data class ParameterStatus(
  val name: String,
  val value: String
) : Message {
  override val id: Int = Companion.id

  companion object :
    Message.Factory<ParameterStatus> {
    override val id: Int = 'S'.toInt()
    override fun decode(source: BufferedSource): ParameterStatus {
      val name = source.readUtf8Terminated()
      val value = source.readUtf8Terminated()
      return ParameterStatus(name, value)
    }
  }
}
