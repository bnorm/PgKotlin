package com.bnorm.pgkotlin.sample

import com.github.jasync.sql.db.Connection
import com.github.jasync.sql.db.postgresql.PostgreSQLConnectionBuilder
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import java.time.Duration
import java.time.Instant

fun main() = runBlocking<Unit>(Dispatchers.Default) {
  val host = "dev-brian-norman.dc.atavium.com"
  val port = 5432
  val database = "postgres"
  val username = "postgres"

  val connection = PostgreSQLConnectionBuilder.createConnectionPool(
    "jdbc:postgresql://$host:$port/$database?user=$username")

  try {
    for (iteration in 1..iterations) {
      println("Query performance iteration $iteration")
      queryPerformance(connection)
    }
  } finally {
    println("Closing client")
    connection.disconnect().await()
  }
}

private suspend fun queryPerformance(client: Connection) {
  var sum = 0L
  val end = Instant.now().plus(duration)
  while (Duration.between(end, Instant.now()).isNegative) {
    client.sendPreparedStatement("SELECT i FROM generate_series(1, 1) AS i").await()
    sum++
  }

  println("$sum queries in ${duration.seconds} secs = ${(sum / duration.seconds)} queries/sec")
}
