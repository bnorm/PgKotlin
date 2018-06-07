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
  database: String = "postgres",
  username: String = "postgres",
  password: String? = null
): PgClient = PgClient(InetSocketAddress(hostname, port), database, username, password)

suspend fun PgClient(
  address: InetSocketAddress,
  database: String = "postgres",
  username: String = "postgres",
  password: String? = null
): PgClient = Connection.connect(address, database, username, password)
