package com.bnorm.pgkotlin.internal.protocol

import com.bnorm.pgkotlin.Portal
import com.bnorm.pgkotlin.QueryExecutor
import com.bnorm.pgkotlin.Result
import com.bnorm.pgkotlin.Row
import com.bnorm.pgkotlin.Statement
import com.bnorm.pgkotlin.Stream
import com.bnorm.pgkotlin.Transaction
import com.bnorm.pgkotlin.internal.PgProtocolException
import com.bnorm.pgkotlin.internal.PostgresSession
import com.bnorm.pgkotlin.internal.PostgresSessionFactory
import com.bnorm.pgkotlin.internal.msg.Authentication
import com.bnorm.pgkotlin.internal.msg.BackendKeyData
import com.bnorm.pgkotlin.internal.msg.Bind
import com.bnorm.pgkotlin.internal.msg.BindComplete
import com.bnorm.pgkotlin.internal.msg.CancelRequest
import com.bnorm.pgkotlin.internal.msg.Close
import com.bnorm.pgkotlin.internal.msg.CloseComplete
import com.bnorm.pgkotlin.internal.msg.CommandComplete
import com.bnorm.pgkotlin.internal.msg.DataRow
import com.bnorm.pgkotlin.internal.msg.Describe
import com.bnorm.pgkotlin.internal.msg.ErrorResponse
import com.bnorm.pgkotlin.internal.msg.Execute
import com.bnorm.pgkotlin.internal.msg.NoData
import com.bnorm.pgkotlin.internal.msg.ParameterStatus
import com.bnorm.pgkotlin.internal.msg.Parse
import com.bnorm.pgkotlin.internal.msg.ParseComplete
import com.bnorm.pgkotlin.internal.msg.PasswordMessage
import com.bnorm.pgkotlin.internal.msg.PortalSuspended
import com.bnorm.pgkotlin.internal.msg.Query
import com.bnorm.pgkotlin.internal.msg.ReadyForQuery
import com.bnorm.pgkotlin.internal.msg.RowDescription
import com.bnorm.pgkotlin.internal.msg.StartupMessage
import com.bnorm.pgkotlin.internal.msg.StatementType
import com.bnorm.pgkotlin.internal.msg.Sync
import com.bnorm.pgkotlin.internal.msg.Terminate
import com.bnorm.pgkotlin.internal.okio.ByteString
import com.bnorm.pgkotlin.internal.okio.IOException
import com.bnorm.pgkotlin.internal.receive
import com.bnorm.pgkotlin.internal.receiveUntil
import com.bnorm.pgkotlin.internal.send
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.withContext

internal class Postgres10(
  private val sessionFactory: PostgresSessionFactory,
  private val encoder: Any?.() -> ByteString?,
  private val scope: CoroutineScope
) : Protocol {
  override suspend fun startup(
    username: String,
    password: String?,
    database: String
  ): Handshake = sessionFactory.session {
    // https://www.postgresql.org/docs/current/static/protocol-flow.html#id-1.10.5.7.3

    // TODO(bnorm) SSL handshake
    send(StartupMessage(username = username, database = database))

    val authentication = receive<Authentication>()
    if (!authentication.success) {
      if (password != null) {
        send(PasswordMessage.create(username, password, authentication.md5salt))
      } else {
        throw IllegalArgumentException("no authentication")
      }
    }

    var processId: Int = -1
    var secretKey: Int = -1
    val parameters = mutableMapOf<String, String>()
    receiveUntil<ReadyForQuery> { msg ->
      when (msg) {
        is ParameterStatus -> parameters[msg.name] = msg.value
        is BackendKeyData -> {
          processId = msg.processId
          secretKey = msg.secretKey
        }
      }
    }

    Handshake(processId, secretKey, parameters)
  }

  override suspend fun terminate(): Unit = sessionFactory.session {
    // https://www.postgresql.org/docs/current/static/protocol-flow.html#id-1.10.5.7.10

    send(Terminate)
  }

  override suspend fun cancel(
    handshake: Handshake
  ): Unit = sessionFactory.session {
    // https://www.postgresql.org/docs/current/static/protocol-flow.html#id-1.10.5.7.10

    // Must be performed on a different connection than the original query
    send(CancelRequest(handshake.processId, handshake.secretKey))
  }

  override suspend fun simpleQuery(
    sql: String
  ): Result? = sessionFactory.session {
    // https://www.postgresql.org/docs/current/static/protocol-flow.html#id-1.10.5.7.4

    send(Query(sql))

    val results = mutableListOf<Row>()
    var description: RowDescription? = null
    receiveUntil<CommandComplete> { msg ->
      when (msg) {
        is DataRow -> results.add(msg.toRow())
        is RowDescription -> description = msg
        else -> throw PgProtocolException("msg=$msg")
      }
    }
    receive<ReadyForQuery>()
    description?.let { Postgres10Result(results) }
  }

  override suspend fun extendedQuery(
    sql: String,
    params: List<Any?>
  ): Result? = sessionFactory.session {
    // https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY

    send(
      Parse(sql),
      Bind(params.map { it.encoder() }),
      Describe(StatementType.Portal),
      Execute(),
      Sync
    )

    receive<ParseComplete>()
    receive<BindComplete>()
    receive<RowDescription>()

    val results = mutableListOf<Row>()
    receiveUntil<CommandComplete> { msg ->
      when (msg) {
        is DataRow -> results.add(msg.toRow())
        else -> throw PgProtocolException("msg=$msg")
      }
    }
    receive<ReadyForQuery>()
    Postgres10Result(results)
  }

  override suspend fun createStatement(
    sql: String,
    name: String
  ): Statement {
    // https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
    require(name.isNotEmpty()) { "Cannot use unnamed prepared statement" }

    return sessionFactory.session {
      send(
        Parse(sql, preparedStatement = name),
        Sync
      )

      receive<ParseComplete>()
      receive<ReadyForQuery>()

      Postgres10Statement(name)
    }
  }

  override suspend fun closeStatement(preparedStatement: String) = sessionFactory.session<Unit> {
    send(
      Close(StatementType.Prepared, preparedStatement),
      Sync
    )
    receive<CloseComplete>()
    receive<ReadyForQuery>()
  }

  override suspend fun createPortal(
    sql: String,
    params: List<Any?>,
    name: String
  ): Portal {
    // https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
    require(name.isNotEmpty()) { "Cannot use unnamed portal" }

    return sessionFactory.session {
      send(
        Parse(sql),
        Bind(params.map { it.encoder() }, portal = name),
        Sync
      )

      receive<ParseComplete>()
      receive<BindComplete>()
      receive<ReadyForQuery>()

      Postgres10Portal(name)
    }
  }

  override suspend fun createStatementPortal(
    preparedStatement: String,
    params: List<Any?>,
    name: String
  ): Portal {
    // https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
    require(name.isNotEmpty()) { "Cannot use unnamed portal" }

    return sessionFactory.session {
      send(
        Bind(
          params.map { it.encoder() },
          preparedStatement = preparedStatement,
          portal = name
        ),
        Sync
      )

      receive<BindComplete>()
      receive<ReadyForQuery>()

      Postgres10Portal(name)
    }
  }

  override suspend fun closePortal(
    name: String
  ) = sessionFactory.session<Unit> {
    send(
      Close(StatementType.Portal, name),
      Sync
    )
    receive<CloseComplete>()
    receive<ReadyForQuery>()
  }

  override suspend fun execute(
    preparedStatement: String,
    params: List<Any?>
  ): Result? = sessionFactory.session {
    // https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY

    send(
      Bind(params.map { it.encoder() }, preparedStatement = preparedStatement),
      Describe(StatementType.Portal),
      Execute(),
      Sync
    )

    receive<BindComplete>()

    receive<RowDescription>()
    val results = mutableListOf<Row>()
    receiveUntil<CommandComplete> { msg ->
      when (msg) {
        is DataRow -> results.add(msg.toRow())
        else -> throw PgProtocolException("msg=$msg")
      }
    }
    receive<ReadyForQuery>()
    Postgres10Result(results)
  }

  override suspend fun stream(
    sql: String,
    params: List<Any?>,
    rows: Int
  ): Stream? = sessionFactory.session {
    // https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY

    send(
      Parse(sql),
      Bind(params.map { it.encoder() }),
      Describe(StatementType.Portal),
      Execute(rows = rows),
      Sync
    )

    receive<ParseComplete>()
    receive<BindComplete>()
    val response = receive()
    when (response) {
      is NoData -> {
        receive<CommandComplete>()
        receive<ReadyForQuery>()
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
  ): Stream? = sessionFactory.session {
    // https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY

    send(
      Bind(params.map { it.encoder() }, preparedStatement = preparedStatement),
      Describe(StatementType.Portal),
      Execute(rows = rows),
      Sync
    )

    receive<BindComplete>()
    val response = receive()
    when (response) {
      is NoData -> {
        receive<CommandComplete>()
        receive<ReadyForQuery>()
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

    return sessionFactory.session {
      send(
        Describe(StatementType.Portal, name = name),
        Execute(name = name, rows = rows),
        Sync
      )

      val response = receive()
      when (response) {
        is NoData -> {
          receive<CommandComplete>()
          receive<ReadyForQuery>()
          null
        }
        is RowDescription -> createPortal(response, rows)
        else -> throw PgProtocolException("msg=$response")
      }
    }
  }

  override suspend fun beginTransaction(executor: QueryExecutor): Transaction {
    simpleQuery("BEGIN TRANSACTION")
    return PgTransaction(executor)
  }

  private fun PostgresSession.createPortal(
    description: RowDescription,
    rows: Int
  ): Stream {
    // Buffer 1 less than the number of possible rows to keep additional
    // executions from being sent
    val data: ReceiveChannel<Row> = scope.produce(
      capacity = (rows - 1).coerceAtLeast(0),
      context = Dispatchers.Unconfined
    ) {
      receiveUntil<CommandComplete> { msg ->
        when (msg) {
          is DataRow -> {
            send(msg.toRow())
          }
          is PortalSuspended -> {
            receive<ReadyForQuery>()
            send(
              Execute(rows = rows),
              Sync
            )
          }
          else -> throw PgProtocolException("msg=$msg")
        }
      }
      receive<ReadyForQuery>()
      send(
        Close(StatementType.Portal),
        Sync
      )
      receive<CloseComplete>()
      receive<ReadyForQuery>()
    }

    return object : Stream, ReceiveChannel<Row> by data {
      override suspend fun close() {
        // Cancel production and close the portal
        // Consume messages until the confirmation of portal closure
        if (!isClosedForReceive) {
          cancel()
          withContext(NonCancellable) {
            send(
              Close(StatementType.Portal),
              Sync
            )
            receiveUntil<CloseComplete> {}
            receive<ReadyForQuery>()
          }
        }
      }
    }
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

private class Postgres10Row(val columns: List<ByteString?>) : Row, List<ByteString?> by columns

private fun DataRow.toRow(): Row {
  return Postgres10Row(values)
}

internal fun ErrorResponse.throwError(): Nothing = throw IOException("$level ($code): $message")