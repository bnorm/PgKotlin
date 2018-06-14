plugins {
  id("org.jetbrains.kotlin.platform.jvm")
}

dependencies {
  implementation(project(":pgkotlin:jvm"))

  implementation(kotlin("stdlib-common"))
  implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:${properties["coroutines_version"]}")

  implementation("io.reactiverse:reactive-pg-client:0.8.0")
}
