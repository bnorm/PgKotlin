package com.bnorm.pgkotlin.internal.msg

enum class StatementType(val code: Int) {
  Prepared('S'.toInt()),
  Portal('P'.toInt()),
}
