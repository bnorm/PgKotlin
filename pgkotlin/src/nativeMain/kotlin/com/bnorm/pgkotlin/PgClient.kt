package com.bnorm.pgkotlin

import com.bnorm.pgkotlin.internal.*

actual interface PgClient : QueryExecutor, NotificationChannel {
  actual suspend fun close()
}

actual suspend fun PgClient(
  hostname: String,
  port: Int,
  database: String,
  username: String,
  password: String?
): PgClient = Connection.connect(hostname, port.toShort(), database, username, password)
