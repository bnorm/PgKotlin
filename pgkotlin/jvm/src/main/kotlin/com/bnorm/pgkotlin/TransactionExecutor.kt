package com.bnorm.pgkotlin

interface TransactionExecutor {

  suspend fun begin(): Transaction
}

interface Transaction : QueryExecutor {

  suspend fun Statement.bind(
    name: String,
    vararg params: Any? = emptyArray()
  ): Portal

  suspend fun Statement.stream(
    vararg params: Any? = emptyArray(),
    batch: Int = 0
  ): Stream?

  suspend fun Portal.stream(
    batch: Int = 0
  ): Stream?

  suspend fun commit()

  suspend fun rollback()
}

suspend inline fun <R> TransactionExecutor.transaction(block: Transaction.() -> R): R {
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
