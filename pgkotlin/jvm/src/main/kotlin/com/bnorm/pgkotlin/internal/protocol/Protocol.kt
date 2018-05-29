package com.bnorm.pgkotlin.internal.protocol

internal interface Protocol {
  suspend fun startup(username: String, password: String?, database: String): Handshake

  suspend fun terminate()

  suspend fun cancel(handshake: Handshake)

  suspend fun simpleQuery(sql: String): Portal?

  suspend fun extendedQuery(sql: String, params: List<Any?>, rows: Int): Portal

  suspend fun createStatement(sql: String, name: String): Statement

  suspend fun createPortal(sql: String, params: List<Any?>, name: String): String

  suspend fun createPortal(statement: Statement, params: List<Any?>, name: String): String

  suspend fun execute(statement: Statement, params: List<Any?>, rows: Int): Portal

  suspend fun executePortal(name: String, rows: Int): Portal
}
