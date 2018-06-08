package com.bnorm.pgkotlin

import org.intellij.lang.annotations.Language

interface QueryExecutor : TransactionExecutor {

  suspend fun prepare(
    @Language("PostgreSQL") sql: String,
    name: String? = null
  ): Statement

  /**
   * $1, $2, etc. for parameters use.
   */
  suspend fun query(
    @Language("PostgreSQL") sql: String,
    vararg params: Any? = emptyArray()
  ): Result?
}
