package com.bnorm.pgkotlin.internal.msg

import kotlinx.io.core.*

/**
 * See [PostgreSQL message formats](https://www.postgresql.org/docs/current/static/protocol-message-formats.html)
 *
 * <pre>
 * ParameterDescription (B)
 *   Byte1('t')
 *     Identifies the message as a parameter description.
 *   Int32
 *     Length of message contents in bytes, including self.
 *   Int16
 *     The number of parameters used by the statement (can be zero).
 *   Then, for each parameter, there is the following:
 *   Int32
 *     Specifies the object ID of the parameter data type.
 * </pre>
 */
internal data class ParameterDescription(
  val types: List<Int>
) : Message {
  override val id = Companion.id

  companion object : Message.Factory<ParameterDescription> {
    override val id = 't'.toByte()
    override fun decode(source: Input): ParameterDescription {
      val count = source.readShort()
      val oids = List(count.toInt()) {
        source.readInt()
      }
      return ParameterDescription(oids)
    }
  }
}
