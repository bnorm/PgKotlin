package com.bnorm.pgkotlin.internal.protocol

import com.bnorm.pgkotlin.*

internal interface Protocol {
  suspend fun startup(username: String, password: String?, database: String): Handshake

  suspend fun terminate()

  suspend fun cancel(handshake: Handshake)

  suspend fun simpleQuery(sql: String): Result?

  suspend fun extendedQuery(sql: String, params: List<Any?>): Result?

  suspend fun createStatement(sql: String, name: String): Statement

  suspend fun closeStatement(preparedStatement: String)

  suspend fun createPortal(sql: String, params: List<Any?>, name: String): Portal

  suspend fun createStatementPortal(
    preparedStatement: String,
    params: List<Any?>,
    name: String
  ): Portal

  suspend fun closePortal(name: String)

  suspend fun execute(preparedStatement: String, params: List<Any?>): Result?

  suspend fun stream(sql: String, params: List<Any?>, rows: Int): Stream?

  suspend fun streamStatement(
    preparedStatement: String,
    params: List<Any?>,
    rows: Int
  ): Stream?

  suspend fun streamPortal(name: String, rows: Int): Stream?

  suspend fun beginTransaction(executor: QueryExecutor): Transaction
}
