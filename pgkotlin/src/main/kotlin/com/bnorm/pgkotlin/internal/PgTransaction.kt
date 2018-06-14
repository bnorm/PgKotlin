package com.bnorm.pgkotlin.internal

import com.bnorm.pgkotlin.*
import com.bnorm.pgkotlin.internal.protocol.Protocol

internal abstract class BaseTransaction(
  private val executor: QueryExecutor,
  private val protocol: Protocol
) : QueryExecutor by executor, Transaction {
  override suspend fun stream(sql: String, vararg params: Any?, batch: Int): Stream? {
    return protocol.stream(sql, params.toList(), batch)
  }

  override suspend fun Statement.bind(name: String, vararg params: Any?): Portal {
    return protocol.createPortal(this, params.toList(), name)
  }

  override suspend fun Statement.stream(vararg params: Any?, batch: Int): Stream? {
    return protocol.stream(this, params.toList(), batch)
  }

  override suspend fun Portal.stream(batch: Int): Stream? {
    return protocol.stream(this, batch)
  }
}

internal class PgTransaction(
  private val executor: QueryExecutor,
  private val protocol: Protocol
) : BaseTransaction(executor, protocol) {

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
) : BaseTransaction(executor, protocol) {
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
