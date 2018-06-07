package com.bnorm.pgkotlin.sample

import com.bnorm.pgkotlin.PgClient
import com.bnorm.pgkotlin.transaction
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.channels.sumBy
import kotlinx.coroutines.experimental.runBlocking
import java.time.Duration
import java.time.Instant
import kotlin.math.roundToInt
import kotlin.system.measureTimeMillis

val clients = 16
val iterations = 10
val rows = 100_000L
val duration = Duration.ofSeconds(5)

fun main(args: Array<String>) = runBlocking<Unit> {
  val clients = List(clients) {
    PgClient(
      hostname = "dev-brian-norman.dc.atavium.com",
      database = "postgres",
      username = "postgres"
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

private suspend fun streamPerformance(clients: List<PgClient>) {
  var sum = 0L
  val time = measureTimeMillis {
    sum = clients.map { client ->
      val statement = client.prepare("SELECT i FROM generate_series(1, $1) AS i")
      async {
        val result = client.transaction {
          execute(statement, rows)!!.sumBy { 1 }.toLong()
        }
        statement.close()
        result
      }
    }.map {
      it.await()
    }.sum()
  }

  println("$sum rows in $time ms = ${(sum / (time / 1000.0)).roundToInt()} rows/sec")
}


private suspend fun queryPerformance(clients: List<PgClient>) {
  val sum = clients.map { client ->
    async {
      val statement = client.prepare("SELECT i FROM generate_series(1, $1) AS i")

      var sum = 0L
      val end = Instant.now().plus(duration)
      while (Duration.between(end, Instant.now()).isNegative) {
        client.transaction { execute(statement, 1L)!!.sumBy { 1 }.toLong() }
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
