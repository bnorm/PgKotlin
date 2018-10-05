package com.bnorm.pgkotlin

import com.bnorm.pgkotlin.internal.okio.ByteString
import kotlinx.coroutines.channels.ReceiveChannel

interface Row : List<ByteString?>

interface Result : List<Row>

interface Stream : ReceiveChannel<Row> {
  suspend fun close()
}
