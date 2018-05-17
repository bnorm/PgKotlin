package com.bnorm.pgkotlin

import okio.ByteString

interface TypeAdapter<T> {
  val oid: Int

  fun decode(value: ByteString): T
  fun encode(value: T): ByteString
}