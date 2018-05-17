import org.jetbrains.kotlin.gradle.dsl.Coroutines
import org.jetbrains.kotlin.gradle.dsl.KotlinJvmCompile
import org.jetbrains.kotlin.gradle.dsl.KotlinProjectExtension
import org.jetbrains.kotlin.gradle.plugin.KotlinPluginWrapper

buildscript {
  repositories {
    jcenter()
    maven { setUrl("http://kotlin.bintray.com/kotlinx") }
    maven { setUrl("https://dl.bintray.com/jetbrains/kotlin-native-dependencies") }
  }
  dependencies {
    classpath("org.jetbrains.kotlin:kotlin-gradle-plugin:1.2.41")
    classpath("org.jetbrains.kotlin:kotlin-native-gradle-plugin:0.6.2")
  }
}

plugins {
  base
  `maven-publish`

  id("nebula.release") version "6.3.0"
  // id("nebula.project") version "3.4.1"
  // id("nebula.maven-publish") version "7.0.1"
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
    configure<KotlinProjectExtension> {
      experimental.coroutines = Coroutines.ENABLE
    }

    tasks.withType<KotlinJvmCompile> {
      kotlinOptions {
        jvmTarget = "1.8"
      }
    }
  }
}
