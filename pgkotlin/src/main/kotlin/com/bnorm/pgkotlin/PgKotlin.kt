package com.bnorm.pgkotlin

import com.bnorm.pgkotlin.internal.Connection
import kotlinx.coroutines.experimental.channels.withIndex
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.runBlocking
import java.util.concurrent.TimeUnit

fun main(args: Array<String>) = runBlocking<Unit> {
  val connection = Connection.connect(
    hostname = "dev-brian-norman.dc.atavium.com",
    username = "postgres",
    database = "postgres"
  )

  connection.transaction {
    // min rows = 37?
    val stream = connection.stream(
      """-- Formatter newline
SELECT *
FROM pg_type""",
      rows = 50
    )!!
    delay(2000)
//    for ((i, row) in stream.withIndex()) {
//      println("row $i = ${row.values}")
//    }
    stream.close()
//    val (columns, rows) = connection.query("SELECT * FROM pg_type")
//    println("${columns.map { it.name }}")
//    for ((i, row) in rows.withIndex()) {
//      println("row $i = ${row.values}")
//    }
  }

  connection.transaction {
    // min rows = 37?
    val stream = connection.stream(
      """-- Formatter newline
SELECT *
FROM pg_type"""
    )!!
    delay(2000)
//    for ((i, row) in stream.withIndex()) {
//      println("row $i = ${row.values}")
//    }
    stream.close()
//    val (columns, rows) = connection.query("SELECT * FROM pg_type")
//    println("${columns.map { it.name }}")
//    for ((i, row) in rows.withIndex()) {
//      println("row $i = ${row.values}")
//    }
  }

  delay(5, TimeUnit.MINUTES)
}
