package com.bnorm.pgkotlin.internal.msg

import com.bnorm.pgkotlin.internal.PgProtocolException
import okio.BufferedSource

/**
 * See [PostgreSQL message formats](https://www.postgresql.org/docs/current/static/protocol-message-formats.html)
 *
 * <pre>
 * ReadyForQuery (B)
 *  Byte1('Z')
 *      Identifies the message type. ReadyForQuery is sent whenever the backend is ready for a new query cycle.
 *  Int32(5)
 *      Length of message contents in bytes, including self.
 *  Byte1
 *      Current backend transaction status indicator. Possible values are 'I' if idle (not in a transaction block); 'T' if in a transaction block; or 'E' if in a failed transaction block (queries will be rejected until block is ended).
 * </pre>
 */
internal data class ReadyForQuery(
  val tx: TxStatus
) : Message {
  override val id: Int = Companion.id

  enum class TxStatus {
    None, Active, Failed
  }

  companion object : Message.Factory<ReadyForQuery> {
    override val id: Int = 'Z'.toInt()

    override fun decode(source: BufferedSource): ReadyForQuery {
      val char = source.readByte().toChar()
      val txStatus = when (char) {
        'I' -> TxStatus.None
        'T' -> TxStatus.Active
        'E' -> TxStatus.Failed
        else -> throw PgProtocolException(msg = "Unknown value: $char")
      }
      return ReadyForQuery(txStatus)
    }
  }
}
