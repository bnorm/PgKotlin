package com.bnorm.pgkotlin.internal.okio

expect class Buffer constructor() : BufferedSource, BufferedSink {
  fun size(): Long
}
