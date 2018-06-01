package com.bnorm.pgkotlin.sample

import com.bnorm.pgkotlin.PgClient
import com.bnorm.pgkotlin.transaction
import kotlinx.coroutines.experimental.CoroutineStart
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.channels.map
import kotlinx.coroutines.experimental.channels.sumBy
import kotlinx.coroutines.experimental.runBlocking
import kotlin.math.roundToInt
import kotlin.system.measureTimeMillis

fun main(args: Array<String>) = runBlocking<Unit> {


  val clients: List<PgClient> = List(2) {
    val client = PgClient(
      hostname = "dev-brian-norman.dc.atavium.com",
      username = "postgres",
      database = "postgres"
    )

    client
  }

  try {
    for (iteration in 1..1) {
      println("Performance iteration $iteration")
      performance(clients)
    }
  } finally {
    println("Closing clients")
    clients.forEach { it.close() }
  }
}

private suspend fun performance(clients: List<PgClient>) {
  val (sum, totalTime) = clients.map { client ->
    val statement = client.prepare("SELECT i FROM generate_series(1, $1) AS i")

    async {
      client.transaction {

        var sum = 0L
        val time = measureTimeMillis {
          val response = execute(statement, 10L)

          sum = response.sumBy { 1 }.toLong()
        }

        sum to time
      }
    }
  }.map {
    it.await()
  }.reduce(::pairwiseSum)

  val time = totalTime / clients.size
  println("$sum rows in $time ms = ${(sum / (time / 1000.0)).roundToInt()} rows/sec")
}

private fun pairwiseSum(
  pair1: Pair<Long, Long>,
  pair2: Pair<Long, Long>
) = (pair1.first + pair2.first) to (pair1.second + pair2.second)
