package com.bnorm.pgkotlin.sample

import io.reactiverse.pgclient.PgClient
import io.reactiverse.pgclient.PgPoolOptions
import io.reactiverse.pgclient.PgResult
import io.reactiverse.pgclient.Row
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import java.time.Duration
import java.time.Instant
import kotlin.coroutines.suspendCoroutine
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

fun main(args: Array<String>) = runBlocking<Unit> {
  // Pool options
  val options = PgPoolOptions()
    .setHost("dev-brian-norman.dc.atavium.com")
    .setPort(5432)
    .setDatabase("postgres")
    .setUser("postgres")
    .setMaxSize(1)

  // Create the client pool
  val client = PgClient.pool(options)

  try {
    for (iteration in 1..iterations) {
      println("Query performance iteration $iteration")
      queryPerformance(client)
    }
  } finally {
    println("Closing client")
    client.close()
  }
}

private suspend fun queryPerformance(client: PgClient) {
  val sum =
    withContext(Dispatchers.Default) {
      var sum = 0L
      val end = Instant.now().plus(duration)
      while (Duration.between(end, Instant.now()).isNegative) {
        client.aQuery("SELECT i FROM generate_series(1, 1) AS i")
        sum ++
      }
      sum
    }

  println("$sum queries in ${duration.seconds} secs = ${(sum / duration.seconds)} queries/sec")
}

suspend fun PgClient.aQuery(sql: String): PgResult<Row> = suspendCoroutine { cont ->
  preparedQuery(sql) { ar ->
    if (ar.succeeded()) {
      cont.resume(ar.result())
    } else {
      cont.resumeWithException(ar.cause())
    }
  }
}
