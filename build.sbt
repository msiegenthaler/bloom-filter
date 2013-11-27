name := "bloom-filter"

scalaVersion := "2.10.3"

version := "1.0.0"


resolvers += "Sonatype OSS Snapshots" at
  "https://oss.sonatype.org/content/repositories/snapshots"


libraryDependencies += "org.specs2" % "specs2_2.10" % "2.2.2" % "test"

libraryDependencies += "com.github.axel22" %% "scalameter" % "0.4" % "test"


testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework")

logBuffered := false
