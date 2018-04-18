package com.bnorm.pgkotlin.internal.msg

import okio.BufferedSink

/**
 * See [PostgreSQL message formats](https://www.postgresql.org/docs/current/static/protocol-message-formats.html)
 *
 * <pre>
 * StartupMessage (F)
 *   Int32
 *     Length of message contents in bytes, including self.
 *   Int32(196608)
 *     The protocol version number. The most significant 16 bits are the major version number (3 for the protocol described here). The least significant 16 bits are the minor version number (0 for the protocol described here).
 *   The protocol version number is followed by one or more pairs of parameter name and value strings. A zero byte is required as a terminator after the last name/value pair. Parameters can appear in any order. user is required, others are optional. Each parameter is specified as:
 *   String
 *     The parameter name. Currently recognized names are:
 *     user
 *       The database user name to connect as. Required; there is no default.
 *     database
 *       The database to connect to. Defaults to the user name.
 *     options
 *       Command-line arguments for the backend. (This is deprecated in favor of setting individual run-time parameters.)
 *     replication
 *       Used to connect in streaming replication mode, where a small set of replication commands can be issued instead of SQL statements. Value can be true, false, or database, and the default is false. See Section 52.4 for details.
 *     In addition to the above, others parameter may be listed. Parameter names beginning with _pq_. are reserved for use as protocol extensions, while others are treated as run-time parameters to be set at backend start time. Such settings will be applied during backend start (after parsing the command-line arguments if any) and will act as session defaults.
 *   String
 *     The parameter value.
 * </pre>
 */
internal data class StartupMessage(
  val protocol: Int = 196608,
  val username: String,
  val database: String
) : Request {
  override val id: Int = -1
  override fun encode(sink: BufferedSink) {
    sink.writeInt(protocol)

    for (str in listOf(
      "user", username,
      "database", database,
      "client_encoding", "UTF8"
    )) {
      sink.write(str.toByteArray())
      sink.writeByte(0)
    }

    sink.writeByte(0)
  }
}
