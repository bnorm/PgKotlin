package com.bnorm.pgkotlin

import com.bnorm.pgkotlin.internal.okio.ByteString
import kotlin.reflect.KClass

abstract class PgType<T : Any>(
  val oid: Int,
  val type: KClass<T>
) {
  abstract fun decode(value: ByteString): T
  abstract fun encode(value: T): ByteString

  override fun toString(): String {
    return "PgType(oid=$oid, type=$type)"
  }
}
