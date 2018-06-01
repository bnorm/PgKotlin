package com.bnorm.pgkotlin.internal

private const val debug = true

internal inline fun debug(block: () -> Unit) {
  if (debug) {
    block()
  }
}
