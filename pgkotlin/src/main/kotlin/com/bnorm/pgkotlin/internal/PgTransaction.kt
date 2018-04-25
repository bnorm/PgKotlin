package com.bnorm.pgkotlin.internal

import com.bnorm.pgkotlin.QueryExecutor
import com.bnorm.pgkotlin.Response
import com.bnorm.pgkotlin.Transaction

internal class PgTransaction(
  private val executor: QueryExecutor
) : Transaction {
  override suspend fun begin(): Transaction {
    executor.query("SAVEPOINT savepoint_0")
    return PgNestedTransaction(executor, 0)
  }

  override suspend fun query(sql: String, vararg params: Any?): Response {
    try {
      return executor.query(sql, params)
    } catch (t: Throwable) {
      rollback()
      throw t
    }
  }

  override suspend fun commit() {
    executor.query("COMMIT TRANSACTION")
  }

  override suspend fun rollback() {
    executor.query("ROLLBACK TRANSACTION")
  }
}

internal class PgNestedTransaction(
  private val executor: QueryExecutor,
  private val depth: Int
) : Transaction {
  override suspend fun begin(): Transaction {
    executor.query("SAVEPOINT savepoint_${depth + 1}")
    return PgNestedTransaction(executor, depth + 1)
  }

  override suspend fun query(sql: String, vararg params: Any?): Response {
    try {
      return executor.query(sql, params)
    } catch (t: Throwable) {
      rollback()
      throw t
    }
  }

  override suspend fun commit() {
    executor.query("RELEASE SAVEPOINT savepoint_$depth")
  }

  override suspend fun rollback() {
    executor.query("ROLLBACK TO SAVEPOINT savepoint_$depth")
  }
}
