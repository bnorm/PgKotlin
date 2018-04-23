package com.bnorm.pgkotlin.internal

import okio.ByteString
import kotlin.reflect.KClass

interface Type<T : Any> {
  val oid: Int
  val type: KClass<T>
  fun decode(value: ByteString): T
  fun encode(value: T): ByteString
}
