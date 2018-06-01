package com.bnorm.pgkotlin.internal.protocol

import com.bnorm.pgkotlin.Portal

internal abstract class NamedPortal(val name: String) : Portal {
  init {
    require(name.isNotEmpty()) { "Cannot use unnamed portal" }
  }
}
