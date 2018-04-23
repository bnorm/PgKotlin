package com.bnorm.pgkotlin

import com.bnorm.pgkotlin.internal.PgProtocolException
import com.bnorm.pgkotlin.internal.Results
import org.intellij.lang.annotations.Language

interface QueryExecutor {

  suspend fun begin(): Transaction

  /**
   * $1, $2, etc. for parameters use.
   */
  suspend fun query(
    @Language("PostgreSQL") sql: String,
    vararg params: Any = emptyArray()
  ): Results
}

interface Transaction : QueryExecutor {

  suspend fun commit()

  suspend fun rollback()
}

suspend inline fun <R> QueryExecutor.transaction(block: QueryExecutor.() -> R): R {
  val txn = begin()
  try {
    val result = block()
    txn.commit()
    return result
  } catch (t: Throwable) {
    if (t !is PgProtocolException) txn.rollback()
    throw t
  }
}
