name := "flink-scala-3"

version := "0.1"

scalaVersion := "3.0.2"

resolvers += Resolver.mavenLocal

javacOptions ++= Seq("-source", "11", "-target", "11")

val flinkVersion = "1.15.1"
libraryDependencies += "org.apache.flink" % "flink-streaming-java" % flinkVersion
libraryDependencies += "org.apache.flink" % "flink-clients" % flinkVersion
libraryDependencies += "org.apache.flink" % "flink-table-planner-loader" % flinkVersion
libraryDependencies += "org.apache.flink" % "flink-table-common" % flinkVersion
libraryDependencies += "org.apache.flink" % "flink-table-api-java" % flinkVersion
libraryDependencies += "org.apache.flink" % "flink-table-api-java-bridge" % flinkVersion
libraryDependencies += "org.apache.flink" % "flink-table-runtime" % flinkVersion
libraryDependencies += "org.apache.flink" % "flink-connector-kafka" % flinkVersion

libraryDependencies += "com.github.losizm" %% "little-json" % "9.0.0"
