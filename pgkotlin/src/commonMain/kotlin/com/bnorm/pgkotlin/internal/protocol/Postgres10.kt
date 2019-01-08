package com.bnorm.pgkotlin.internal.protocol

import com.bnorm.pgkotlin.*
import com.bnorm.pgkotlin.internal.*
import com.bnorm.pgkotlin.internal.msg.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*

internal class Postgres10(
  private val requests: SendChannel<Request>,
  private val responses: ReceiveChannel<Message>,
  private val encoder: Any?.() -> ByteArray?,
  private val scope: CoroutineScope
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

    val results = mutableListOf<Row>()
    var description: RowDescription? = null
    for (msg in responses) {
      if (msg is CommandComplete) {
        responses.receive<ReadyForQuery>()
        break
      }
      when (msg) {
        is DataRow -> results.add(msg.toRow())
        is RowDescription -> description = msg
        else -> throw PgProtocolException("msg=$msg")
      }
    }
    return description?.let { Postgres10Result(results) }
  }

  override suspend fun extendedQuery(
    sql: String,
    params: List<Any?>
  ): Result? {
    // https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY

    requests.send(
      Parse(sql),
      Bind(params.map { it.encoder() }),
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

    val results = mutableListOf<Row>()
    for (msg in responses) {
      if (msg is CommandComplete) {
        responses.receive<ReadyForQuery>()
        break
      }
      when (msg) {
        is DataRow -> results.add(msg.toRow())
        else -> throw PgProtocolException("msg=$msg")
      }
    }
    return Postgres10Result(results)
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

    return Postgres10Statement(name)
  }

  override suspend fun closeStatement(preparedStatement: String) {
    requests.send(
      Close(StatementType.Prepared, preparedStatement),
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
      Bind(params.map { it.encoder() }, portal = name),
      Sync
    )

    responses.receive<ParseComplete>()
    responses.receive<BindComplete>()
    responses.receive<ReadyForQuery>()

    return Postgres10Portal(name)
  }

  override suspend fun createStatementPortal(
    preparedStatement: String,
    params: List<Any?>,
    name: String
  ): Portal {
    // https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
    require(name.isNotEmpty()) { "Cannot use unnamed portal" }

    requests.send(
      Bind(
        params.map { it.encoder() },
        preparedStatement = preparedStatement,
        portal = name
      ),
      Sync
    )

    responses.receive<BindComplete>()
    responses.receive<ReadyForQuery>()

    return Postgres10Portal(name)
  }

  override suspend fun closePortal(
    name: String
  ) {
    requests.send(
      Close(StatementType.Portal, name),
      Sync
    )
    responses.receive<CloseComplete>()
    responses.receive<ReadyForQuery>()
  }

  override suspend fun execute(
    preparedStatement: String,
    params: List<Any?>
  ): Result? {
    // https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY

    requests.send(
      Bind(params.map { it.encoder() }, preparedStatement = preparedStatement),
      Describe(StatementType.Portal),
      Execute(),
      Sync
    )

    responses.receive<BindComplete>()

    val description = responses.receive()
    if (description !is RowDescription) {
      throw PgProtocolException("msg=$description")
    }

    val results = mutableListOf<Row>()
    for (msg in responses) {
      if (msg is CommandComplete) {
        responses.receive<ReadyForQuery>()
        break
      }
      when (msg) {
        is DataRow -> results.add(msg.toRow())
        else -> throw PgProtocolException("msg=$msg")
      }
    }
    return Postgres10Result(results)
  }

  override suspend fun stream(
    sql: String,
    params: List<Any?>,
    rows: Int
  ): Stream? {
    // https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY

    requests.send(
      Parse(sql),
      Bind(params.map { it.encoder() }),
      Describe(StatementType.Portal),
      Execute(rows = rows),
      Sync
    )

    responses.receive<ParseComplete>()
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

  override suspend fun streamStatement(
    preparedStatement: String,
    params: List<Any?>,
    rows: Int
  ): Stream? {
    // https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY

    requests.send(
      Bind(params.map { it.encoder() }, preparedStatement = preparedStatement),
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

  override suspend fun streamPortal(
    name: String,
    rows: Int
  ): Stream? {
    // https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
    require(name.isNotEmpty()) { "Cannot use unnamed portal" }

    requests.send(
      Describe(StatementType.Portal, name = name),
      Execute(name = name, rows = rows),
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

  override suspend fun beginTransaction(executor: QueryExecutor): Transaction {
    simpleQuery("BEGIN TRANSACTION")
    return PgTransaction(executor)
  }

  private fun createPortal(
    description: RowDescription,
    rows: Int
  ): Stream {
    // Buffer 1 less than the number of possible rows to keep additional
    // executions from being sent
    val data: ReceiveChannel<Row> = scope.produce(
      capacity = (rows - 1).coerceAtLeast(0),
      context = Dispatchers.Unconfined
    ) {
      for (msg in responses) {
        when (msg) {
          is DataRow -> {
            send(msg.toRow())
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

    return object : Stream, ReceiveChannel<Row> by data {
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
    }
  }

  private suspend inline fun <reified T> ReceiveChannel<Message>.receive(
  ): T {
    val msg = withTimeout(30_000) { receive() }
    return msg as? T ?: throw Exception("unexpected=$msg")
  }

  private suspend inline fun <reified T : Message> ReceiveChannel<Message>.receiveUntil(
  ): T {
    for (msg in this) {
      if (msg is T) return msg
    }
    throw PgProtocolException("not found = ${T::class}")
  }

  private inner class Postgres10Statement(val name: String) : Statement {
    override suspend fun query(vararg params: Any?): Result? {
      return execute(name, params.toList())
    }

    override suspend fun close() {
      closeStatement(name)
    }
  }

  private inner class Postgres10Portal(val name: String) : Portal {
    override suspend fun close() {
      closePortal(name)
    }
  }

  private abstract inner class BaseTransaction(
    private val executor: QueryExecutor
  ) : QueryExecutor by executor, Transaction {
    override suspend fun stream(sql: String, vararg params: Any?, batch: Int): Stream? {
      return this@Postgres10.stream(sql, params.toList(), batch)
    }

    override suspend fun Statement.bind(name: String, vararg params: Any?): Portal {
      require(this is Postgres10Statement)
      return createStatementPortal(this.name, params.toList(), name)
    }

    override suspend fun Statement.stream(vararg params: Any?, batch: Int): Stream? {
      require(this is Postgres10Statement)
      return streamStatement(this.name, params.toList(), batch)
    }

    override suspend fun Portal.stream(batch: Int): Stream? {
      require(this is Postgres10Portal)
      return streamPortal(this.name, batch)
    }
  }

  private inner class PgTransaction(
    private val executor: QueryExecutor
  ) : BaseTransaction(executor) {

    override suspend fun begin(): Transaction {
      simpleQuery("SAVEPOINT savepoint_0")
      return PgNestedTransaction(executor, 0)
    }

    override suspend fun commit() {
      simpleQuery("COMMIT TRANSACTION")
    }

    override suspend fun rollback() {
      simpleQuery("ROLLBACK TRANSACTION")
    }
  }

  private inner class PgNestedTransaction(
    private val executor: QueryExecutor,
    private val depth: Int
  ) : BaseTransaction(executor) {
    override suspend fun begin(): Transaction {
      simpleQuery("SAVEPOINT savepoint_${depth + 1}")
      return PgNestedTransaction(executor, depth + 1)
    }

    override suspend fun commit() {
      simpleQuery("RELEASE SAVEPOINT savepoint_$depth")
    }

    override suspend fun rollback() {
      simpleQuery("ROLLBACK TO SAVEPOINT savepoint_$depth")
    }
  }
}

private class Postgres10Result(val rows: List<Row>) : Result, List<Row> by rows

private class Postgres10Row(val columns: List<ByteArray?>) : Row, List<ByteArray?> by columns

private fun DataRow.toRow(): Row {
  return Postgres10Row(values)
}
