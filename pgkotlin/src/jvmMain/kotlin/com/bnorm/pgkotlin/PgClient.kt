package com.bnorm.pgkotlin

import com.bnorm.pgkotlin.internal.Connection
import java.net.InetSocketAddress

actual interface PgClient : QueryExecutor, NotificationChannel {
  actual suspend fun close()
}

actual suspend fun PgClient(
  hostname: String,
  port: Int,
  database: String,
  username: String,
  password: String?
): PgClient = PgClient(InetSocketAddress(hostname, port), database, username, password)

suspend fun PgClient(
  address: InetSocketAddress,
  database: String = "postgres",
  username: String = "postgres",
  password: String? = null
): PgClient = Connection.connect(address, database, username, password)
