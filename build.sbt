name := "bloom-filter"

scalaVersion := "2.10.2"

version := "1.0.0"


libraryDependencies += "org.specs2" % "specs2_2.10" % "2.2.2" % "test"

libraryDependencies += "com.github.axel22" %% "scalameter" % "0.4-M2" % "test"


testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework")

logBuffered := false
