import org.jetbrains.kotlin.gradle.dsl.Coroutines
import org.jetbrains.kotlin.gradle.dsl.KotlinJvmCompile
import org.jetbrains.kotlin.gradle.plugin.KotlinPluginWrapper

plugins {
  kotlin("jvm") version "1.2.41"

  id("nebula.release") version "6.3.0"
  // id("nebula.project") version "3.4.1"
  // id("nebula.maven-publish") version "7.0.1"
  `maven-publish`
}

group = "com.bnorm.pgkotlin"

//release {
//  defaultVersionStrategy = Strategies.SNAPSHOT
//}
tasks["release"].dependsOn(tasks["build"])
tasks["release"].finalizedBy(tasks["publish"])

allprojects {
  repositories {
    jcenter()
  }

  plugins.withType<KotlinPluginWrapper> {
    kotlin {
      experimental.coroutines = Coroutines.ENABLE
    }

    tasks.withType<KotlinJvmCompile> {
      kotlinOptions {
        jvmTarget = "1.8"
      }
    }
  }
}
