package com.bnorm.pgkotlin.internal.msg

import com.bnorm.pgkotlin.internal.PgType
import com.bnorm.pgkotlin.internal.okio.BufferedSource
import com.bnorm.pgkotlin.internal.toPgType

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
  val types: List<PgType<*>>
) : Message {
  override val id: Int = Companion.id

  companion object : Message.Factory<ParameterDescription> {
    override val id: Int = 't'.toInt()
    override fun decode(source: BufferedSource): ParameterDescription {
      val count = source.readShort()
      val oids = List(count.toInt()) {
        source.readInt().toPgType()
      }
      return ParameterDescription(oids)
    }
  }
}
