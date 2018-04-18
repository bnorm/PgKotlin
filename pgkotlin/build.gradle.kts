import org.jetbrains.kotlin.gradle.dsl.Coroutines

plugins {
  kotlin("jvm")
}

dependencies {
  compile(kotlin("stdlib-jdk8"))
  compile("org.jetbrains.kotlinx:kotlinx-coroutines-core:0.22.5")
  compile("org.jetbrains.kotlinx:kotlinx-coroutines-nio:0.22.5")
  compile("com.squareup.okio:okio:1.14.0")
}

kotlin {
  experimental.coroutines = Coroutines.ENABLE
}
