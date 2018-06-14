plugins {
  id("org.jetbrains.kotlin.platform.jvm")
}

dependencies {
  expectedBy(project(":pgkotlin"))

  implementation(kotlin("stdlib-jdk8"))
  implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:${properties["coroutines_version"]}")
  implementation("org.jetbrains.kotlinx:kotlinx-coroutines-nio:${properties["coroutines_version"]}")
  implementation("com.squareup.okio:okio:1.14.0")
}
