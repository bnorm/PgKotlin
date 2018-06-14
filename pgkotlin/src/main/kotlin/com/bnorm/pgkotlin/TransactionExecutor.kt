package com.bnorm.pgkotlin

interface Transaction : QueryExecutor {

  suspend fun stream(
    sql: String,
    vararg params: Any? = emptyArray(),
    batch: Int = 0
  ): Stream?

  suspend fun Statement.stream(
    vararg params: Any? = emptyArray(),
    batch: Int = 0
  ): Stream?

  suspend fun Statement.bind(
    name: String,
    vararg params: Any? = emptyArray()
  ): Portal

  suspend fun Portal.stream(
    batch: Int = 0
  ): Stream?

  suspend fun commit()

  suspend fun rollback()
}

suspend inline fun <R> QueryExecutor.transaction(block: Transaction.() -> R): R {
  val txn = begin()
  try {
    val result = txn.block()
    txn.commit()
    return result
  } catch (t: Throwable) {
    // TODO? rollback?
    throw t
  }
}
