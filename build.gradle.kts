import org.jetbrains.kotlin.gradle.dsl.Coroutines
import org.jetbrains.kotlin.gradle.dsl.KotlinJvmProjectExtension
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
  kotlin("jvm") version "1.2.31"

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
}

tasks.withType<KotlinCompile> {
  kotlinOptions {
    jvmTarget = "1.8"
  }
}
