package com.bnorm.pgkotlin.internal.protocol

import com.bnorm.pgkotlin.internal.PgProtocolException
import com.bnorm.pgkotlin.internal.msg.*
import com.bnorm.pgkotlin.internal.pgEncode
import kotlinx.coroutines.experimental.NonCancellable
import kotlinx.coroutines.experimental.Unconfined
import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.channels.SendChannel
import kotlinx.coroutines.experimental.channels.produce
import kotlinx.coroutines.experimental.withContext
import kotlinx.coroutines.experimental.withTimeout
import java.util.concurrent.TimeUnit

internal class Postgres10(
  private val requests: SendChannel<Request>,
  private val responses: ReceiveChannel<Message>
) : Protocol {
  override suspend fun startup(username: String, password: String?, database: String): Handshake {
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

    var processId: Int = -1
    var secretKey: Int = -1
    val parameters = mutableMapOf<String, String>()
    for (msg in responses) {
      if (msg is ReadyForQuery) break

      if (msg is ParameterStatus) {
        parameters[msg.name] = msg.value
      }
      if (msg is BackendKeyData) {
        processId = msg.processId
        secretKey = msg.secretKey
      }
    }

    return Handshake(processId, secretKey, parameters)
  }

  override suspend fun terminate() {
    // https://www.postgresql.org/docs/current/static/protocol-flow.html#id-1.10.5.7.10

    requests.send(Terminate)
  }

  override suspend fun cancel(handshake: Handshake) {
    // https://www.postgresql.org/docs/current/static/protocol-flow.html#id-1.10.5.7.10

    // Must be performed on a different connection than the original query
    requests.send(CancelRequest(handshake.processId, handshake.secretKey))
  }

  override suspend fun simpleQuery(sql: String): Portal? {
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

  override suspend fun extendedQuery(sql: String, params: List<Any?>, rows: Int): Portal {
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

  override suspend fun createStatement(sql: String, name: String): Statement {
    // https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
    require(name.isNotEmpty()) { "Cannot use unnamed prepared statement" }

    requests.send(Parse(sql, preparedStatement = name))
    requests.send(Sync)

    responses.receive<ParseComplete>()
    responses.receive<ReadyForQuery>()

    return object : Statement(name) {
      override suspend fun close() {
        requests.send(Close(StatementType.Prepared, name))
        requests.send(Sync)
        responses.receive<CloseComplete>()
        responses.receive<ReadyForQuery>()
      }
    }
  }

  override suspend fun createPortal(
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

  override suspend fun createPortal(
    statement: Statement,
    params: List<Any?>,
    name: String
  ): String {
    // https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
    require(name.isNotEmpty()) { "Cannot use unnamed portal" }

    requests.send(
      Bind(
        params.map { it.pgEncode() },
        preparedStatement = statement.name,
        portal = name
      )
    )
    requests.send(Sync)

    responses.receive<BindComplete>()
    responses.receive<ReadyForQuery>()

    return name
  }

  override suspend fun execute(
    statement: Statement,
    params: List<Any?>,
    rows: Int
  ): Portal {
    // https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY

    requests.send(Bind(params.map { it.pgEncode() }, preparedStatement = statement.name))
    // Flush for early BindComplete?

    requests.send(Describe(StatementType.Portal))
    // Flush for early RowDescription?

    requests.send(Execute(rows = rows))
    requests.send(Sync)

    responses.receive<BindComplete>()
    val description = responses.receive<RowDescription>()

    return createPortal(description, rows)
  }

  override suspend fun executePortal(name: String, rows: Int): Portal {
    // https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
    require(name.isNotEmpty()) { "Cannot use unnamed portal" }

    requests.send(Describe(StatementType.Portal, name = name))
    // Flush for early RowDescription?

    requests.send(Execute(name = name, rows = rows))
    requests.send(Sync)

    val description = responses.receive<RowDescription>()

    return createPortal(description, rows)
  }


  private fun createPortal(
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
            responses.receiveUntil<CloseComplete>()
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

  private suspend inline fun <reified T : Message> ReceiveChannel<Message>.receiveUntil(): T {
    for (msg in this) {
      if (msg is T) return msg
    }
    throw PgProtocolException("not found = ${T::class}")
  }

}
