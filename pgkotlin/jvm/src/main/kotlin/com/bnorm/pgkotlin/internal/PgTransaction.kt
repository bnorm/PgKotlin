package com.bnorm.pgkotlin.internal

import com.bnorm.pgkotlin.*
import com.bnorm.pgkotlin.internal.protocol.Protocol

internal abstract class BaseTransaction(
  private val executor: QueryExecutor
) : Transaction, QueryExecutor by executor {

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
  private val executor: QueryExecutor,
  private val protocol: Protocol
  ) : BaseTransaction(executor) {

  override suspend fun begin(): Transaction {
    executor.query("SAVEPOINT savepoint_0")
    return PgNestedTransaction(executor, protocol, 0)
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
  private val protocol: Protocol,
  private val depth: Int
) : BaseTransaction(executor) {
  override suspend fun begin(): Transaction {
    executor.query("SAVEPOINT savepoint_${depth + 1}")
    return PgNestedTransaction(executor, protocol, depth + 1)
  }

  override suspend fun commit() {
    executor.query("RELEASE SAVEPOINT savepoint_$depth")
  }

  override suspend fun rollback() {
    executor.query("ROLLBACK TO SAVEPOINT savepoint_$depth")
  }
}
