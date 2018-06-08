plugins {
  id("org.jetbrains.kotlin.platform.jvm")
}

dependencies {
  implementation(project(":pgkotlin:jvm"))

  implementation(kotlin("stdlib-common"))
  implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:0.23.0-dev-1")
}
