plugins {
  id("org.jetbrains.kotlin.platform.common")
}

dependencies {
  implementation(kotlin("stdlib-common"))
  implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core-common:${properties["coroutines_version"]}")

  testImplementation(kotlin("test-annotations-common"))
  testImplementation(kotlin("test-common"))
}
