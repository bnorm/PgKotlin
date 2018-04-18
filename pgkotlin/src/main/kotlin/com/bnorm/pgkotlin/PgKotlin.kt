package com.bnorm.pgkotlin

import com.bnorm.pgkotlin.internal.Connection
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.runBlocking
import java.util.concurrent.TimeUnit

fun main(args: Array<String>) = runBlocking<Unit> {
  val connection = Connection.connect(
    hostname = "dev-brian-norman.dc.atavium.com",
    username = "postgres",
    database = "postgres"
  )

  println("connection = ${connection}")

  connection.transaction {
    val (columns, rows) = connection.query("SELECT * FROM hoard.reservations")
    println("${columns.map { it.name }}")
    for ((i, row) in rows.withIndex()) {
      println("row $i = ${row.values}")
    }
  }

  delay(5, TimeUnit.MINUTES)
}
