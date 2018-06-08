plugins {
  id("org.jetbrains.kotlin.platform.jvm")
}

dependencies {
  expectedBy(project(":pgkotlin"))

  implementation(kotlin("stdlib-jdk8"))
  implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:0.23.0")
  implementation("org.jetbrains.kotlinx:kotlinx-coroutines-nio:0.23.0")
  implementation("com.squareup.okio:okio:1.14.0")
}
