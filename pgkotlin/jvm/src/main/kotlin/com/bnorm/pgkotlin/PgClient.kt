package com.bnorm.pgkotlin

import org.intellij.lang.annotations.Language

interface PgClient : TransactionExecutor, NotificationChannel, AutoCloseable {
  suspend fun prepare(
    @Language("PostgreSQL") sql: String,
    name: String? = null
  ): Statement
}
