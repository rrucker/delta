name := "DeltaTest"

version := "0.1"

scalaVersion := "2.12.10"

def getDeltaVersion(): String = {
val envVars = System.getenv
if (envVars.containsKey("DELTA_VERSION")) {
val version = envVars.get("DELTA_VERSION")
println("Using Delta version " + version)
version
} else {
"0.7.0"
}
}

lazy val root = (project in file("."))
.settings(
name := "hello-world"

libraryDependencies += "com.couchbase.client" %% "scala-client" % "1.0.9"


libraryDependencies += "io.delta" %% "delta-core" % getDeltaVersion()
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.0",
resolvers += "Delta" at "https://dl.bintray.com/delta-io/delta/")
libraryDependencies += "log4j" % "log4j" % "1.2.17"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.0.1"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "3.0.1"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.0.1"

libraryDependencies += "io.delta" %% "delta-core" % "0.7.0"

libraryDependencies += "org.creativescala" %% "doodle" % "0.9.20"
libraryDependencies += "org.plotly-scala" %% "plotly-core" % "0.7.6"
libraryDependencies += "org.plotly-scala" %% "plotly-render" % "0.7.6"
fork in run := true
