package com.bnorm.pgkotlin.internal.protocol

import com.bnorm.pgkotlin.Portal
import com.bnorm.pgkotlin.Result
import com.bnorm.pgkotlin.Statement
import com.bnorm.pgkotlin.Stream
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
  override suspend fun startup(
    username: String,
    password: String?,
    database: String
  ): Handshake {
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

  override suspend fun terminate(
  ) {
    // https://www.postgresql.org/docs/current/static/protocol-flow.html#id-1.10.5.7.10

    requests.send(Terminate)
  }

  override suspend fun cancel(
    handshake: Handshake
  ) {
    // https://www.postgresql.org/docs/current/static/protocol-flow.html#id-1.10.5.7.10

    // Must be performed on a different connection than the original query
    requests.send(CancelRequest(handshake.processId, handshake.secretKey))
  }

  override suspend fun simpleQuery(
    sql: String
  ): Result? {
    // https://www.postgresql.org/docs/current/static/protocol-flow.html#id-1.10.5.7.4

    requests.send(Query(sql))

    val results = mutableListOf<DataRow>()
    var description: RowDescription? = null
    for (msg in responses) {
      if (msg is CommandComplete) {
        responses.receive<ReadyForQuery>()
        break
      }
      when (msg) {
        is DataRow -> results.add(msg)
        is RowDescription -> description = msg
        else -> throw PgProtocolException("msg=$msg")
      }
    }
    return description?.let { Result(results) }
  }

  override suspend fun extendedQuery(
    sql: String,
    params: List<Any?>
  ): Result? {
    // https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY

    requests.send(
      Parse(sql),
      Bind(params.map { it.pgEncode() }),
      Describe(StatementType.Portal),
      Execute(),
      Sync
    )

    responses.receive<ParseComplete>()
    responses.receive<BindComplete>()

    val description = responses.receive()
    if (description !is RowDescription) {
      throw PgProtocolException("msg=$description")
    }

    val results = mutableListOf<DataRow>()
    for (msg in responses) {
      if (msg is CommandComplete) {
        responses.receive<ReadyForQuery>()
        break
      }
      when (msg) {
        is DataRow -> results.add(msg)
        else -> throw PgProtocolException("msg=$msg")
      }
    }
    return Result(results)
  }

  override suspend fun createStatement(
    sql: String,
    name: String
  ): Statement {
    // https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
    require(name.isNotEmpty()) { "Cannot use unnamed prepared statement" }

    requests.send(
      Parse(sql, preparedStatement = name),
      Sync
    )

    responses.receive<ParseComplete>()
    responses.receive<ReadyForQuery>()

    return Statement(name, this)
  }

  override suspend fun closeStatement(statement: Statement) {
    requests.send(
      Close(StatementType.Prepared, statement.name),
      Sync
    )
    responses.receive<CloseComplete>()
    responses.receive<ReadyForQuery>()
  }

  override suspend fun createPortal(
    sql: String,
    params: List<Any?>,
    name: String
  ): Portal {
    // https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
    require(name.isNotEmpty()) { "Cannot use unnamed portal" }

    requests.send(
      Parse(sql),
      Bind(params.map { it.pgEncode() }, portal = name),
      Sync
    )

    responses.receive<ParseComplete>()
    responses.receive<BindComplete>()
    responses.receive<ReadyForQuery>()

    return Portal(name, this)
  }

  override suspend fun createPortal(
    statement: Statement,
    params: List<Any?>,
    name: String
  ): Portal {
    // https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
    require(name.isNotEmpty()) { "Cannot use unnamed portal" }

    requests.send(
      Bind(
        params.map { it.pgEncode() },
        preparedStatement = statement.name,
        portal = name
      ),
      Sync
    )

    responses.receive<BindComplete>()
    responses.receive<ReadyForQuery>()

    return Portal(name, this)
  }

  override suspend fun closePortal(
    portal: Portal
  ) {
    requests.send(
      Close(StatementType.Portal, portal.name),
      Sync
    )
    responses.receive<CloseComplete>()
    responses.receive<ReadyForQuery>()
  }

  override suspend fun execute(
    statement: Statement,
    params: List<Any?>
  ): Result? {
    // https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY

    requests.send(
      Bind(params.map { it.pgEncode() }, preparedStatement = statement.name),
      Describe(StatementType.Portal),
      Execute(),
      Sync
    )

    responses.receive<BindComplete>()

    val description = responses.receive()
    if (description !is RowDescription) {
      throw PgProtocolException("msg=$description")
    }

    val results = mutableListOf<DataRow>()
    for (msg in responses) {
      if (msg is CommandComplete) {
        responses.receive<ReadyForQuery>()
        break
      }
      when (msg) {
        is DataRow -> results.add(msg)
        else -> throw PgProtocolException("msg=$msg")
      }
    }
    return Result(results)
  }

  override suspend fun stream(
    statement: Statement,
    params: List<Any?>,
    rows: Int
  ): Stream? {
    // https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY

    requests.send(
      Bind(params.map { it.pgEncode() }, preparedStatement = statement.name),
      Describe(StatementType.Portal),
      Execute(rows = rows),
      Sync
    )

    responses.receive<BindComplete>()
    val response = responses.receive()
    return when (response) {
      is NoData -> {
        responses.receive<CommandComplete>()
        responses.receive<ReadyForQuery>()
        null
      }
      is RowDescription -> createPortal(response, rows)
      else -> throw PgProtocolException("msg=$response")
    }
  }

  override suspend fun stream(
    portal: Portal,
    rows: Int
  ): Stream? {
    // https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
    require(portal.name.isNotEmpty()) { "Cannot use unnamed portal" }

    requests.send(
      Describe(StatementType.Portal, name = portal.name),
      Execute(name = portal.name, rows = rows),
      Sync
    )

    val response = responses.receive()
    return when (response) {
      is NoData -> {
        responses.receive<CommandComplete>()
        responses.receive<ReadyForQuery>()
        null
      }
      is RowDescription -> createPortal(response, rows)
      else -> throw PgProtocolException("msg=$response")
    }
  }


  private fun createPortal(
    description: RowDescription,
    rows: Int
  ): Stream {
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
            requests.send(
              Execute(rows = rows),
              Sync
            )
          }
          is CommandComplete -> {
            responses.receive<ReadyForQuery>()
            requests.send(
              Close(StatementType.Portal),
              Sync
            )
            responses.receive<CloseComplete>()
            responses.receive<ReadyForQuery>()
            return@produce
          }
          else -> throw PgProtocolException("msg=$msg")
        }
      }
    }

    return Stream(object : RowStream(description, data) {
      override suspend fun close() {
        // Cancel production and close the portal
        // Consume messages until the confirmation of portal closure
        if (!isClosedForReceive) {
          cancel()
          withContext(NonCancellable) {
            requests.send(
              Close(StatementType.Portal),
              Sync
            )
            responses.receiveUntil<CloseComplete>()
            responses.receive<ReadyForQuery>()
          }
        }
      }
    })
  }

  private suspend inline fun <reified T> ReceiveChannel<Message>.receive(
    timeout: Long = 30,
    timeUnit: TimeUnit = TimeUnit.SECONDS
  ): T {
    val msg = withTimeout(timeout, timeUnit) { receive() }
    return msg as? T ?: throw Exception("unexpected=$msg")
  }

  private suspend inline fun <reified T : Message> ReceiveChannel<Message>.receiveUntil(
  ): T {
    for (msg in this) {
      if (msg is T) return msg
    }
    throw PgProtocolException("not found = ${T::class}")
  }
}
