package com.bnorm.pgkotlin

import com.bnorm.pgkotlin.internal.PgProtocolException
import com.bnorm.pgkotlin.internal.debug
import org.intellij.lang.annotations.Language

interface TransactionExecutor {

  suspend fun begin(): Transaction
}

interface Transaction : QueryExecutor {

  suspend fun create(
    statement: Statement,
    vararg params: Any? = emptyArray(),
    name: String? = null
  ): Portal

  suspend fun create(
    @Language("PostgreSQL") sql: String,
    vararg params: Any? = emptyArray(),
    name: String? = null
  ): Portal

  suspend fun commit()

  suspend fun rollback()
}

suspend inline fun <R> TransactionExecutor.transaction(block: QueryExecutor.() -> R): R {
  val txn = begin()
  try {
    val result = txn.block()
    txn.commit()
    return result
  } catch (t: Throwable) {
    // TODO? rollback?
    if (t !is PgProtocolException && t !is NotImplementedError) {
      txn.rollback()
    }
    throw t
  }
}
