plugins {
  id("konan")
}

konanArtifacts {
  library("native") {
    enableMultiplatform(true)
  }

//  program("native-test") {
//    srcDir("src/test/kotlin")
//    commonSourceSets("test")
//    libraries {
//      artifact("native")
//    }
//    extraOpts("-tr")
//  }
}

dependencies {
  expectedBy(project(":pgkotlin"))
}
