plugins {
  id("org.jetbrains.kotlin.platform.jvm")
}

dependencies {
  expectedBy(project(":pgkotlin"))

  implementation(kotlin("stdlib-jdk8"))
  implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:0.23.0-dev-1")
  implementation("org.jetbrains.kotlinx:kotlinx-coroutines-nio:0.23.0-dev-1")
  implementation("org.jetbrains.kotlinx:kotlinx-coroutines-io:0.23.0-dev-1")
  implementation("org.jetbrains.kotlinx:kotlinx-io-jvm:0.0.10")
  implementation("com.squareup.okio:okio:1.14.0")
}
