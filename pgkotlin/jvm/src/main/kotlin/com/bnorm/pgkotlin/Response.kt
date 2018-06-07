package com.bnorm.pgkotlin

import com.bnorm.pgkotlin.internal.msg.DataRow
import com.bnorm.pgkotlin.internal.protocol.RowStream
import kotlinx.coroutines.experimental.channels.ReceiveChannel

class Response internal constructor(
  private val rows: RowStream
) : ReceiveChannel<DataRow> by rows {
  suspend fun close() {
    rows.close()
  }
}
