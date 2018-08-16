package com.bnorm.pgkotlin

import kotlinx.coroutines.experimental.channels.BroadcastChannel

interface NotificationChannel {
  suspend fun listen(channel: String): BroadcastChannel<String>
}
