package com.bnorm.pgkotlin

expect interface PgClient : QueryExecutor, NotificationChannel {
  suspend fun close()
}

expect suspend fun PgClient(
  hostname: String,
  port: Int = 5432,
  database: String = "postgres",
  username: String = "postgres",
  password: String? = null
): PgClient
