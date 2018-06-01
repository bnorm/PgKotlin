package com.bnorm.pgkotlin

import com.bnorm.pgkotlin.internal.Connection
import org.intellij.lang.annotations.Language
import java.net.InetSocketAddress

interface PgClient : TransactionExecutor, NotificationChannel, AutoCloseable {
  suspend fun prepare(
    @Language("PostgreSQL") sql: String,
    name: String? = null
  ): Statement
}

suspend fun PgClient(
  hostname: String,
  port: Int = 5432,
  username: String = "postgres",
  password: String? = null,
  database: String = "postgres"
): PgClient = Connection.connect(InetSocketAddress(hostname, port), username, password, database)

suspend fun PgClient(
  address: InetSocketAddress,
  username: String = "postgres",
  password: String? = null,
  database: String = "postgres"
): PgClient = Connection.connect(address, username, password, database)
