package com.bnorm.pgkotlin

import kotlinx.coroutines.flow.Flow

interface Row : List<Any?>
interface Column {
  val name: String
  val type: PgType<*>
}

interface Result : List<Row> {
  val columns: List<Column>
}

interface Stream : Flow<Row> {
  val columns: List<Column>
  suspend fun cancel()
}
