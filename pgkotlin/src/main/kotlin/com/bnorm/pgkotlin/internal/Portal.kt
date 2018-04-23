package com.bnorm.pgkotlin.internal

import com.bnorm.pgkotlin.internal.msg.DataRow
import com.bnorm.pgkotlin.internal.msg.RowDescription
import kotlinx.coroutines.experimental.channels.ReceiveChannel

abstract class Portal internal constructor(
  val rowDescription: RowDescription,
  private val dataRows: ReceiveChannel<DataRow>
) : ReceiveChannel<DataRow> by dataRows {
  abstract suspend fun close()
}
