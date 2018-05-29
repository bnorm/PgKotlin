package com.bnorm.pgkotlin.internal.protocol

import com.bnorm.pgkotlin.Statement

internal abstract class NamedStatement(val name: String) : Statement {
  init {
    require(name.isNotEmpty()) { "Cannot use unnamed prepared statement" }
  }
}
