package com.bnorm.pgkotlin

interface Statement {
  suspend fun close()
}
