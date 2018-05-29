package com.bnorm.pgkotlin.internal

const val debug = true

inline fun debug(block: () -> Unit) {
  if (debug) {
    block()
  }
}
