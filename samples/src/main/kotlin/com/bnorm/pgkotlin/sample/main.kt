package com.bnorm.pgkotlin.sample

import com.bnorm.pgkotlin.PgClient
import com.bnorm.pgkotlin.QueryExecutor
import com.bnorm.pgkotlin.transaction
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.sumBy
import kotlinx.coroutines.runBlocking
import java.time.Duration
import java.time.Instant
import kotlin.math.roundToInt
import kotlin.system.measureTimeMillis

val clients = 1
val iterations = 10
val rows = 100_000L
val batch = 5000
val duration = Duration.ofSeconds(5)

fun main(args: Array<String>): Unit = runBlocking {
  val clients = List(clients) {
    PgClient(
      hostname = "dev-brian-norman.dc.atavium.com",
      port = 5432,
      database = "postgres",
      username = "postgres",
      password = null
    )
  }

  try {
    for (iteration in 1..iterations) {
      println("Query performance iteration $iteration")
      queryPerformance(clients)
    }

    for (iteration in 1..iterations) {
      println("Stream performance iteration $iteration")
      streamPerformance(clients)
    }
  } finally {
    println("Closing clients")
    clients.forEach { it.close() }
  }
}

private suspend fun streamPerformance(clients: List<QueryExecutor>) {
  var sum = 0L
  val time = measureTimeMillis {
    sum = clients.map { client ->
      async {
        client.transaction {
          stream("SELECT i FROM generate_series(1, $1) AS i", rows, batch = batch)!!.sumBy { 1 }.toLong()
        }
      }
    }.map {
      it.await()
    }.sum()
  }

  println("$sum rows in $time ms = ${(sum / (time / 1000.0)).roundToInt()} rows/sec")
}


private suspend fun queryPerformance(clients: List<QueryExecutor>) {
  val sum = clients.map { client ->
    async {
      val statement = client.prepare("SELECT i FROM generate_series(1, $1) AS i")

      var sum = 0L
      val end = Instant.now().plus(duration)
      while (Duration.between(end, Instant.now()).isNegative) {
        statement.query(1L)
        sum++
      }

      statement.close()
      sum
    }
  }.map {
    it.await()
  }.sum()

  println("$sum queries in ${duration.seconds} secs = ${(sum / duration.seconds)} queries/sec")
}
