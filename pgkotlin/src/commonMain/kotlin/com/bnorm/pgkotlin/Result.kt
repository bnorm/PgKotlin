package com.bnorm.pgkotlin

import kotlinx.coroutines.channels.*

interface Row : List<ByteArray?>

interface Result : List<Row>

interface Stream : ReceiveChannel<Row> {
  suspend fun close()
}
