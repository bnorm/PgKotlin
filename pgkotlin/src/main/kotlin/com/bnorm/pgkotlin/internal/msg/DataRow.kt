package com.bnorm.pgkotlin.internal.msg

import com.bnorm.pgkotlin.internal.okio.BufferedSource
import com.bnorm.pgkotlin.internal.okio.ByteString

/**
 * See [PostgreSQL message formats](https://www.postgresql.org/docs/current/static/protocol-message-formats.html)
 *
 * <pre>
 * DataRow (B)
 *   Byte1('D')
 *     Identifies the message as a data row.
 *   Int32
 *     Length of message contents in bytes, including self.
 *   Int16
 *     The number of column values that follow (possibly zero).
 *   Next, the following pair of fields appear for each column:
 *   Int32
 *     The length of the column value, in bytes (this count does not include itself). Can be zero. As a special case, -1 indicates a NULL column value. No value bytes follow in the NULL case.
 *   Byten
 *     The value of the column, in the format indicated by the associated format code. n is the above length.
 * </pre>
 */
internal data class DataRow(
  val values: List<ByteString?>
) : Message {
  override val id: Int = Companion.id

  companion object : Message.Factory<DataRow> {
    override val id: Int = 'D'.toInt()
    override fun decode(source: BufferedSource): DataRow {
      val len = source.readShort().toInt()
      val values = List(len) {
        val length = source.readInt()
        if (length != -1) source.readByteString(length.toLong())
        else null
      }
      return DataRow(values)
    }
  }
}
