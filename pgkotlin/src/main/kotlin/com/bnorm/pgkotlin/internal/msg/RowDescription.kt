package com.bnorm.pgkotlin.internal.msg

import com.bnorm.pgkotlin.internal.okio.BufferedSource

/**
 * See [PostgreSQL message formats](https://www.postgresql.org/docs/current/static/protocol-message-formats.html)
 *
 * <pre>
 * RowDescription (B)
 *   Byte1('T')
 *     Identifies the message as a row description.
 *   Int32
 *     Length of message contents in bytes, including self.
 *   Int16
 *     Specifies the number of fields in a row (can be zero).
 *   Then, for each field, there is the following:
 *   String
 *     The field name.
 *   Int32
 *     If the field can be identified as a column of a specific table, the object ID of the table; otherwise zero.
 *   Int16
 *     If the field can be identified as a column of a specific table, the attribute number of the column; otherwise zero.
 *   Int32
 *     The object ID of the field's data type.
 *   Int16
 *     The data type size (see pg_type.typlen). Note that negative values denote variable-width types.
 *   Int32
 *     The type modifier (see pg_attribute.atttypmod). The meaning of the modifier is type-specific.
 *   Int16
 *     The format code being used for the field. Currently will be zero (text) or one (binary). In a RowDescription returned from the statement variant of Describe, the format code is not yet known and will always be zero.
 * </pre>
 */
internal data class RowDescription(
  val columns: List<ColumnDescription>
) : Message {
  override val id: Int = Companion.id

  companion object : Message.Factory<RowDescription> {
    override val id: Int = 'T'.toInt()
    override fun decode(source: BufferedSource): RowDescription {
      val len = source.readShort().toInt()
      val columns = List(len) {
        val name = source.readUtf8Terminated()
        val objectId = source.readInt()
        val attributeNumber = source.readShort().toInt()
        val type = source.readInt()
        val typeLength = source.readShort().toInt()
        val typeModifier = source.readInt()
        val formatCode = source.readShort().toInt()
        ColumnDescription(
          name,
          objectId,
          attributeNumber,
          type,
          typeLength,
          typeModifier,
          FormatType.valueOf(formatCode)
        )
      }
      return RowDescription(columns)
    }
  }
}

internal data class ColumnDescription(
  val name: String,
  val objectId: Int,
  val attributeNumber: Int,
  val type: Int,
  val typeLength: Int,
  val typeModifier: Int,
  val format: FormatType
)
