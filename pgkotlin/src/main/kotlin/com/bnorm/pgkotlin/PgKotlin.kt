package com.bnorm.pgkotlin

import com.bnorm.pgkotlin.internal.Connection
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.channels.sumBy
import kotlinx.coroutines.experimental.runBlocking
import kotlin.math.roundToInt
import kotlin.system.measureTimeMillis

fun main(args: Array<String>) = runBlocking<Unit> {
  //  val connection = Connection.connect(
//    hostname = "dev-brian-norman.dc.atavium.com",
//    username = "postgres",
//    database = "postgres"
//  )
//
//  connection.transaction {
//    // min rows = 37?
//    val stream = connection.stream(
//      """-- Formatter newline
//SELECT *
//FROM pg_type""",
//      rows = 50
//    )
//    delay(2000)
////    for ((i, row) in stream.withIndex()) {
////      println("row $i = ${row.values}")
////    }
//    stream.close()
////    val (columns, rows) = connection.query("SELECT * FROM pg_type")
////    println("${columns.map { it.name }}")
////    for ((i, row) in rows.withIndex()) {
////      println("row $i = ${row.values}")
////    }
//  }
//
//  connection.transaction {
//    // min rows = 37?
//    val stream = connection.stream(
//      """-- Formatter newline
//SELECT *
//FROM pg_type"""
//    )
//    delay(2000)
////    for ((i, row) in stream.withIndex()) {
////      println("row $i = ${row.values}")
////    }
//    stream.close()
////    val (columns, rows) = connection.query("SELECT * FROM pg_type")
////    println("${columns.map { it.name }}")
////    for ((i, row) in rows.withIndex()) {
////      println("row $i = ${row.values}")
////    }
//  }
//
//  delay(5, TimeUnit.MINUTES)


  val connections = List(16) {
    Connection.connect(
      hostname = "dev-brian-norman.dc.atavium.com",
      username = "postgres",
      database = "postgres"
    )
  }

  try {
    for (iteration in 1..10) {
      println("Performance iteration $iteration")
      performance(connections)
    }
  } finally {
    println("Closing connections")
    connections.forEach { it.close() }
  }
}

private suspend fun performance(connections: List<Connection>) {
  val (sum, totalTime) = connections.map { connection ->
    async {
      connection.transaction {
        var sum = 0L
        val time = measureTimeMillis {
          val response = connection.stream(
            "SELECT i FROM generate_series(1, $1) AS i",
            100_000L,
            rows = 5_000
          ) as Response.Stream

          sum = response.sumBy { 1 }.toLong()
        }

        sum to time
      }

//        val response = connection.query(
//          "SELECT i FROM generate_series(1, $1) AS i",
//          100_000L
//        ) as Response.Complete
//
//        response.size.toLong()
    }
  }.map {
    it.await()
  }.reduce(::pairwiseSum)

  val time = totalTime / connections.size
  println("$sum rows in $time ms = ${(sum / (time / 1000.0)).roundToInt()} rows/sec")
}

private fun pairwiseSum(
  pair1: Pair<Long, Long>,
  pair2: Pair<Long, Long>
) = (pair1.first + pair2.first) to (pair1.second + pair2.second)
