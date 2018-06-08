package com.bnorm.pgkotlin.internal

private const val debug = false

internal inline fun debug(block: () -> Unit) {
  if (debug) {
    block()
  }
}
