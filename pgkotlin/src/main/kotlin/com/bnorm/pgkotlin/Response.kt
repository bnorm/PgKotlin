package com.bnorm.pgkotlin

import com.bnorm.pgkotlin.internal.Portal
import com.bnorm.pgkotlin.internal.msg.DataRow
import com.bnorm.pgkotlin.internal.msg.RowDescription
import kotlinx.coroutines.experimental.channels.ReceiveChannel

sealed class Response {
  abstract suspend fun close()

  object Empty : Response() {
    override suspend fun close() {}
  }

  class Complete internal constructor(
    private val columns: RowDescription,
    private val rows: List<DataRow>
  ) : Response(), List<DataRow> by rows {
    override suspend fun close() {}
  }

  class Stream internal constructor(
    private val rows: Portal
  ) : Response(), ReceiveChannel<DataRow> by rows {
    override suspend fun close() {
      rows.close()
    }
  }
}
