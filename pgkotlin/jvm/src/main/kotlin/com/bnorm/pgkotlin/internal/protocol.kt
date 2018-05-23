package com.bnorm.pgkotlin.internal

import com.bnorm.pgkotlin.internal.msg.*
import kotlinx.coroutines.experimental.NonCancellable
import kotlinx.coroutines.experimental.Unconfined
import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.channels.SendChannel
import kotlinx.coroutines.experimental.channels.produce
import kotlinx.coroutines.experimental.withContext
import kotlinx.coroutines.experimental.withTimeout
import java.util.concurrent.TimeUnit

internal class Something(
  val requests: SendChannel<Request>,
  val responses: ReceiveChannel<Message>
)

internal abstract class Portal internal constructor(
  val description: RowDescription,
  private val rows: ReceiveChannel<DataRow>
) : ReceiveChannel<DataRow> by rows {
  abstract suspend fun close()
}


internal suspend fun Something.startup(
  username: String = "postgres",
  password: String? = null,
  database: String = "postgres"
) {
  // https://www.postgresql.org/docs/current/static/protocol-flow.html#id-1.10.5.7.3

  // TODO(bnorm) SSL handshake?
  requests.send(StartupMessage(username = username, database = database))

  val authentication = responses.receive() as? Authentication ?: throw PgProtocolException()
  if (!authentication.success) {
    if (password != null) {
      requests.send(PasswordMessage.create(username, password, authentication.md5salt))
    } else {
      throw IllegalArgumentException("no authentication")
    }
  }

  // responses consume BackendKeyData
  // responses consume ParameterStatus

  responses.receive<ReadyForQuery>()
}

internal suspend fun Something.terminate() {
  // https://www.postgresql.org/docs/current/static/protocol-flow.html#id-1.10.5.7.10

  requests.send(Terminate)
}

internal suspend fun Something.cancel(
  processId: Int,
  secretKey: Int
) {
  // https://www.postgresql.org/docs/current/static/protocol-flow.html#id-1.10.5.7.10

  // Must be performed on a different connection than the original query
  requests.send(CancelRequest(processId, secretKey))
}

internal suspend fun Something.simpleQuery(
  sql: String
): Portal? {
  // https://www.postgresql.org/docs/current/static/protocol-flow.html#id-1.10.5.7.4

  requests.send(Query(sql))

  val response = responses.receive()
  return when (response) {
    is EmptyQueryResponse -> {
      responses.receive<ReadyForQuery>()
      null
    }
    is CommandComplete -> {
      responses.receive<ReadyForQuery>()
      null
    }
    is RowDescription -> createPortal(response, 0)
    else -> throw PgProtocolException("msg=$response")
  }
}

internal suspend fun Something.extendedQuery(
  sql: String,
  params: List<Any?>,
  rows: Int
): Portal {
  // https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY

  requests.send(Parse(sql))
  // Flush for early ParseComplete?

  requests.send(Bind(params.map { it.pgEncode() }))
  // Flush for early BindComplete?

  requests.send(Describe(StatementType.Portal))
  // Flush for early RowDescription?

  requests.send(Execute(rows = rows))
  requests.send(Sync)

  responses.receive<ParseComplete>()
  responses.receive<BindComplete>()
  val description = responses.receive<RowDescription>()

  return createPortal(description, rows)
}

internal suspend fun Something.createPreparedStatement(
  sql: String,
  name: String
): String {
  // https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
  require(name.isNotEmpty()) { "Cannot use unnamed prepared statement" }

  requests.send(Parse(sql, preparedStatement = name))
  requests.send(Sync)

  responses.receive<ParseComplete>()
  responses.receive<ReadyForQuery>()

  return name
}

internal suspend fun Something.createPortal(
  sql: String,
  params: List<Any?>,
  name: String
): String {
  // https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
  require(name.isNotEmpty()) { "Cannot use unnamed portal" }

  requests.send(Parse(sql))
  // Flush for early BindComplete?

  requests.send(Bind(params.map { it.pgEncode() }, portal = name))
  requests.send(Sync)

  responses.receive<ParseComplete>()
  responses.receive<BindComplete>()
  responses.receive<ReadyForQuery>()

  return name
}

internal suspend fun Something.createPortalFromPreparedStatement(
  preparedStatementName: String,
  params: List<Any?>,
  portalName: String
): String {
  // https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
  require(portalName.isNotEmpty()) { "Cannot use unnamed portal" }

  requests.send(
    Bind(
      params.map { it.pgEncode() },
      preparedStatement = preparedStatementName,
      portal = portalName
    )
  )
  requests.send(Sync)

  responses.receive<BindComplete>()
  responses.receive<ReadyForQuery>()

  return portalName
}

internal suspend fun Something.executePreparedStatement(
  name: String,
  params: List<Any?>,
  rows: Int
): Portal {
  // https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
  require(name.isNotEmpty()) { "Cannot use unnamed prepared statement" }

  requests.send(Bind(params.map { it.pgEncode() }, preparedStatement = name))
  // Flush for early BindComplete?

  requests.send(Describe(StatementType.Portal))
  // Flush for early RowDescription?

  requests.send(Execute(rows = rows))
  requests.send(Sync)

  responses.receive<BindComplete>()
  val description = responses.receive<RowDescription>()

  return createPortal(description, rows)
}

internal suspend fun Something.executePortal(
  name: String,
  rows: Int
): Portal {
  // https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
  require(name.isNotEmpty()) { "Cannot use unnamed portal" }

  requests.send(Describe(StatementType.Portal, name = name))
  // Flush for early RowDescription?

  requests.send(Execute(name = name, rows = rows))
  requests.send(Sync)

  val description = responses.receive<RowDescription>()

  return createPortal(description, rows)
}

private fun Something.createPortal(
  description: RowDescription,
  rows: Int
): Portal {
  // Buffer 1 less than the number of possible rows to keep additional
  // executions from being sent
  val data = produce<DataRow>(
    capacity = (rows - 1).coerceAtLeast(0),
    context = Unconfined
  ) {
    for (msg in responses) {
      when (msg) {
        is DataRow -> {
          send(msg)
        }
        is PortalSuspended -> {
          responses.receive<ReadyForQuery>()
          requests.send(Execute(rows = rows))
          requests.send(Sync)
        }
        is CommandComplete -> {
          responses.receive<ReadyForQuery>()
          requests.send(Close(StatementType.Portal))
          requests.send(Sync)
          responses.receive<CloseComplete>()
          responses.receive<ReadyForQuery>()
          return@produce
        }
        else -> throw PgProtocolException("msg=$msg")
      }
    }
  }

  return object : Portal(description, data) {
    override suspend fun close() {
      // Cancel production and close the portal
      // Consume messages until the confirmation of portal closure
      if (!isClosedForReceive) {
        cancel()
        withContext(NonCancellable) {
          requests.send(Close(StatementType.Portal))
          requests.send(Sync)
          responses.consumeUntil<CloseComplete>()
          responses.receive<ReadyForQuery>()
        }
      }
    }
  }
}

private suspend inline fun <reified T> ReceiveChannel<Message>.receive(
  timeout: Long = 30,
  timeUnit: TimeUnit = TimeUnit.SECONDS
): T {
  val msg = withTimeout(timeout, timeUnit) { receive() }
  return msg as? T ?: throw PgProtocolException("unexpected=$msg")
}

private suspend inline fun <reified T : Message> ReceiveChannel<Message>.consumeUntil(): T {
  for (msg in this) {
    if (msg is T) return msg
  }
  throw PgProtocolException("not found = ${T::class}")
}
