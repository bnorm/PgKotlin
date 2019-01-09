package com.bnorm.pgkotlin

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlin.math.*
import kotlin.system.*
import kotlin.test.*

// ./gradlew nativeTest
class PerformanceTest {
  private val clients = 1
  private val iterations = 1_000

  @Test
  fun queryPerf() = runBlocking {
    println("Starting query performance test")

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
        queryPerformance(clients, 5_000 /* milli */)
      }
    } finally {
      println("Closing clients")
      clients.forEach { it.close() }
    }
  }

  private suspend fun queryPerformance(clients: List<QueryExecutor>, duration: Int): Unit = coroutineScope {
    val sum = clients.map { client ->
      async {
        val statement = client.prepare("SELECT i FROM generate_series(1, $1) AS i")

        var sum = 0L
        val end = getTimeMillis() + duration
        while (getTimeMillis() < end) {
          statement.query(1L)
          sum++
        }

        statement.close()
        sum
      }
    }.map {
      it.await()
    }.sum()

    println("$sum queries in ${duration / 1_000} secs = ${(sum * 1_000 / duration)} queries/sec")
  }

  @Test
  fun streamPerf() = runBlocking {
    println("Starting stream performance test")

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
        println("Stream performance iteration $iteration")
        streamPerformance(clients, 10L, 5000)
      }
    } finally {
      println("Closing clients")
      clients.forEach { it.close() }
    }
  }

  private suspend fun streamPerformance(clients: List<QueryExecutor>, rows: Long, batch: Int): Unit = coroutineScope {
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
}
