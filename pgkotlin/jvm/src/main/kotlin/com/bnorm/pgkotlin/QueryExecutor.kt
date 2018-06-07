package com.bnorm.pgkotlin

import org.intellij.lang.annotations.Language

interface QueryExecutor : TransactionExecutor {

  /**
   * $1, $2, etc. for parameters use.
   */
  suspend fun query(
    @Language("PostgreSQL") sql: String,
    vararg params: Any? = emptyArray()
  ): Response?

  suspend fun execute(
    statement: Statement,
    vararg params: Any? = emptyArray()
  ): Response?

  suspend fun execute(
    portal: Portal
  ): Response?
}
