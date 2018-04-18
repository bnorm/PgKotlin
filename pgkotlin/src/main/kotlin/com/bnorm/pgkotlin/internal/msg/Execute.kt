package com.bnorm.pgkotlin.internal.msg

import okio.BufferedSink
import okio.BufferedSource

/**
 * See [PostgreSQL message formats](https://www.postgresql.org/docs/current/static/protocol-message-formats.html)
 *
 * <pre>
 * Execute (F)
 *   Byte1('E')
 *     Identifies the message as an Execute command.
 *   Int32
 *     Length of message contents in bytes, including self.
 *   String
 *     The name of the portal to execute (an empty string selects the unnamed portal).
 *   Int32
 *     Maximum number of rows to return, if portal contains a query that returns rows (ignored otherwise). Zero denotes "no limit".
 * </pre>
 */
internal object Execute : Request {
  override val id: Int = 'E'.toInt()
  override fun encode(sink: BufferedSink) {
    sink.writeByte(0)
    sink.writeInt(0)
  }
}
