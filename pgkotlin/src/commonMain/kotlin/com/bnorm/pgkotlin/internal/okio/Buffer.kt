package com.bnorm.pgkotlin.internal.okio

expect class Buffer constructor() : BufferedSource, BufferedSink {
  var size: Long
    internal set
}
