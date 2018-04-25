package com.bnorm.pgkotlin

import com.bnorm.pgkotlin.internal.Connection
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.channels.sumBy
import kotlinx.coroutines.experimental.runBlocking
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


  var sum: Long = 0
  val time = measureTimeMillis {
    sum = List(50) {
      async {
        val connection = Connection.connect(
          hostname = "dev-brian-norman.dc.atavium.com",
          username = "postgres",
          database = "postgres"
        )

        connection.transaction {
          val response = connection.stream(
            "SELECT i FROM generate_series(1, $1) AS i",
            100_000L,
            rows = 1_000
          ) as Response.Stream

          response.sumBy { 1 }.toLong()
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
    }.sum()
  }
  println("$sum rows in $time ms = ${sum / (time / 1000.0)} rows/sec")
}
