package com.bnorm.pgkotlin

interface Statement {
  suspend fun query(vararg params: Any?): Result?

  suspend fun close()
}
