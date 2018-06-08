plugins {
  id("org.jetbrains.kotlin.platform.common")
}

dependencies {
  implementation(kotlin("stdlib-common"))
  implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core-common:0.23.0")
  implementation("org.jetbrains.kotlinx:kotlinx-io:0.0.10")
}
