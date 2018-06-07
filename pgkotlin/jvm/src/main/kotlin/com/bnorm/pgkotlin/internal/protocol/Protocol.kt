package com.bnorm.pgkotlin.internal.protocol

internal interface Protocol {
  suspend fun startup(username: String, password: String?, database: String): Handshake

  suspend fun terminate()

  suspend fun cancel(handshake: Handshake)

  suspend fun simpleQuery(sql: String): RowStream?

  suspend fun extendedQuery(sql: String, params: List<Any?>, rows: Int): RowStream?

  suspend fun createStatement(sql: String, name: String): NamedStatement

  suspend fun createPortal(sql: String, params: List<Any?>, name: String): NamedPortal

  suspend fun createPortal(statement: NamedStatement, params: List<Any?>, name: String): NamedPortal

  suspend fun execute(statement: NamedStatement, params: List<Any?>, rows: Int): RowStream?

  suspend fun execute(portal: NamedPortal, rows: Int): RowStream?
}
