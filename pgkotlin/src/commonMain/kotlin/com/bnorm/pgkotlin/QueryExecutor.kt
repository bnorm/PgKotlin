package com.bnorm.pgkotlin

interface QueryExecutor {

  suspend fun prepare(
    sql: String,
    name: String? = null
  ): Statement

  /**
   * $1, $2, etc. for parameters use.
   */
  suspend fun query(
    sql: String,
    vararg params: Any? = emptyArray()
  ): Result?

  suspend fun begin(): Transaction
}
