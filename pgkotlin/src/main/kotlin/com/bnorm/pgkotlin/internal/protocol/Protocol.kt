package com.bnorm.pgkotlin.internal.protocol

import com.bnorm.pgkotlin.Statement
import com.bnorm.pgkotlin.Portal
import com.bnorm.pgkotlin.Result
import com.bnorm.pgkotlin.Stream

internal interface Protocol {
  suspend fun startup(username: String, password: String?, database: String): Handshake

  suspend fun terminate()

  suspend fun cancel(handshake: Handshake)

  suspend fun simpleQuery(sql: String): Result?

  suspend fun extendedQuery(sql: String, params: List<Any?>): Result?

  suspend fun createStatement(sql: String, name: String): Statement

  suspend fun closeStatement(statement: Statement)

  suspend fun createPortal(sql: String, params: List<Any?>, name: String): Portal

  suspend fun createPortal(statement: Statement, params: List<Any?>, name: String): Portal

  suspend fun closePortal(portal: Portal)

  suspend fun execute(statement: Statement, params: List<Any?>): Result?

  suspend fun stream(sql: String, params: List<Any?>, rows: Int): Stream?

  suspend fun stream(statement: Statement, params: List<Any?>, rows: Int): Stream?

  suspend fun stream(portal: Portal, rows: Int): Stream?
}
