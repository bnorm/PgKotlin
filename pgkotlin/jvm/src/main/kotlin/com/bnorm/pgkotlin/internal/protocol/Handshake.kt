package com.bnorm.pgkotlin.internal.protocol

internal data class Handshake(
  val processId: Int,
  val secretKey: Int,
  val parameters: Map<String, String>
)