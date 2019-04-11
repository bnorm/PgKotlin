package com.bnorm.pgkotlin.sample

import io.reactiverse.kotlin.pgclient.commitAwait
import io.reactiverse.kotlin.pgclient.executeAwait
import io.reactiverse.kotlin.pgclient.getConnectionAwait
import io.reactiverse.kotlin.pgclient.prepareAwait
import io.reactiverse.kotlin.pgclient.preparedQueryAwait
import io.reactiverse.kotlin.pgclient.queryAwait
import io.reactiverse.kotlin.pgclient.readAwait
import io.reactiverse.pgclient.PgClient
import io.reactiverse.pgclient.PgConnection
import io.reactiverse.pgclient.PgPool
import io.reactiverse.pgclient.PgPoolOptions
import io.reactiverse.pgclient.Tuple
import io.reactiverse.pgclient.impl.ArrayTuple
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.fold
import kotlinx.coroutines.flow.reduce
import kotlinx.coroutines.runBlocking
import java.time.Duration
import java.time.Instant
import kotlin.math.roundToInt
import kotlin.system.measureTimeMillis

fun main() = runBlocking<Unit>(Dispatchers.Default) {
  // Pool options
  val options = PgPoolOptions()
    .setHost("dev-brian-norman.dc.atavium.com")
    .setPort(5432)
    .setDatabase("postgres")
    .setUser("postgres")

  // Create the client pool
  val pool = PgClient.pool(options)

  try {
    val clients = (0 until clients).map { pool.getConnectionAwait() }
    for (iteration in 1..iterations) {
      println("Query performance iteration $iteration")
      queryPerformance(clients[0])
    }

//    for (iteration in 1..iterations) {
//      println("Stream performance iteration $iteration")
//      streamPerformance(pool)
//    }
  } finally {
    println("Closing pool")
    pool.close()
  }
}

private suspend fun queryPerformance(client: PgConnection) {
  val statement = client.prepareAwait("SELECT i FROM generate_series(1, 1) AS i")
  var sum = 0L
  val end = Instant.now().plus(duration)
  while (Duration.between(end, Instant.now()).isNegative) {
    statement.executeAwait()
    sum++
  }

  println("$sum queries in ${duration.seconds} secs = ${(sum / duration.seconds)} queries/sec")
}

@UseExperimental(FlowPreview::class)
private suspend fun streamPerformance(pool: PgPool): Unit = coroutineScope {
  var sum = 0L
  val time = measureTimeMillis {
    sum = (0 until clients).map {
      async {
        val client = pool.getConnectionAwait()
        val query = client.prepareAwait("SELECT i FROM generate_series(1, 1000000) AS i")
        val txn = client.begin()
        try {
          flow {
            val cursor = query.cursor()
            var more = true
            while (more) {
              val rowSet = cursor.readAwait(batch)
              for (row in rowSet) {
                emit(row)
              }
              more = cursor.hasMore()
            }
          }.fold(0) { r, _ -> r + 1 }.toLong()
        } finally {
          txn.commitAwait()
          client.close()
        }
      }
    }.map {
      it.await()
    }.sum()
  }

  println("$sum rows in $time ms = ${(sum / (time / 1000.0)).roundToInt()} rows/sec")
}
