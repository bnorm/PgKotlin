package com.bnorm.pgkotlin.internal.protocol

import com.bnorm.pgkotlin.internal.msg.DataRow
import com.bnorm.pgkotlin.internal.msg.RowDescription
import kotlinx.coroutines.experimental.channels.ReceiveChannel

internal abstract class Portal internal constructor(
  val description: RowDescription,
  private val rows: ReceiveChannel<DataRow>
) : ReceiveChannel<DataRow> by rows {
  abstract suspend fun close()
}
