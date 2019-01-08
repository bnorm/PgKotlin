package com.bnorm.pgkotlin.internal.msg

enum class StatementType(val code: Byte) {
  Prepared('S'.toByte()),
  Portal('P'.toByte()),
}
