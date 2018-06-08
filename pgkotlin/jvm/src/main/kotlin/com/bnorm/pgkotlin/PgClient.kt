package com.bnorm.pgkotlin

import com.bnorm.pgkotlin.internal.Connection
import java.net.InetSocketAddress

interface PgClient : QueryExecutor, NotificationChannel, AutoCloseable

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
