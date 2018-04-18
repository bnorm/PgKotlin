package com.bnorm.pgkotlin

import com.bnorm.pgkotlin.internal.PgType

data class Column<T : Any>(val name: String, val type: PgType<T>)

data class Row(val values: List<Any?>)

data class Results internal constructor(
  val colums: List<Column<*>>,
  val rows: List<Row>
)
