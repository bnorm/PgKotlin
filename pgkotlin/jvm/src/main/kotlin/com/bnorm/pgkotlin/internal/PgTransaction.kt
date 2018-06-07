package com.bnorm.pgkotlin.internal

import com.bnorm.pgkotlin.Portal
import com.bnorm.pgkotlin.QueryExecutor
import com.bnorm.pgkotlin.Statement
import com.bnorm.pgkotlin.Transaction
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
    protocol.simpleQuery("SAVEPOINT savepoint_0")
    return PgNestedTransaction(executor, protocol, 0)
  }

  override suspend fun commit() {
    protocol.simpleQuery("COMMIT TRANSACTION")
  }

  override suspend fun rollback() {
    protocol.simpleQuery("ROLLBACK TRANSACTION")
  }
}

internal class PgNestedTransaction(
  private val executor: QueryExecutor,
  private val protocol: Protocol,
  private val depth: Int
) : BaseTransaction(executor) {
  override suspend fun begin(): Transaction {
    protocol.simpleQuery("SAVEPOINT savepoint_${depth + 1}")
    return PgNestedTransaction(executor, protocol, depth + 1)
  }

  override suspend fun commit() {
    protocol.simpleQuery("RELEASE SAVEPOINT savepoint_$depth")
  }

  override suspend fun rollback() {
    protocol.simpleQuery("ROLLBACK TO SAVEPOINT savepoint_$depth")
  }
}
