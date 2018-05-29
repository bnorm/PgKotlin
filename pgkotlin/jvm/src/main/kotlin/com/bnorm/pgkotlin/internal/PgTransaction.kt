package com.bnorm.pgkotlin.internal

import com.bnorm.pgkotlin.*

internal abstract class BaseTransaction(
  private val executor: QueryExecutor
) : Transaction {

  final override suspend fun query(sql: String, vararg params: Any?): Response {
    try {
      return executor.query(sql, *params)
    } catch (t: Throwable) {
      rollback()
      throw t
    }
  }

  final override suspend fun execute(statement: Statement, vararg params: Any?): Response {
    try {
      return executor.execute(statement, params)
    } catch (t: Throwable) {
      rollback()
      throw t
    }
  }

  final override suspend fun execute(portal: Portal): Response {
    try {
      return executor.execute(portal)
    } catch (t: Throwable) {
      rollback()
      throw t
    }
  }

  final override suspend fun create(
    statement: Statement,
    vararg params: Any?,
    name: String?
  ): Portal {
    TODO("not implemented")
  }

  final override suspend fun create(sql: String, vararg params: Any?, name: String?): Portal {
    TODO("not implemented")
  }
}

internal class PgTransaction(
  private val executor: QueryExecutor
) : BaseTransaction(executor) {

  override suspend fun begin(): Transaction {
    executor.query("SAVEPOINT savepoint_0")
    return PgNestedTransaction(executor, 0)
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
) : BaseTransaction(executor) {
  override suspend fun begin(): Transaction {
    executor.query("SAVEPOINT savepoint_${depth + 1}")
    return PgNestedTransaction(executor, depth + 1)
  }

  override suspend fun commit() {
    executor.query("RELEASE SAVEPOINT savepoint_$depth")
  }

  override suspend fun rollback() {
    executor.query("ROLLBACK TO SAVEPOINT savepoint_$depth")
  }
}
