name := "flink-scala-3"

version := "0.1"

scalaVersion := "3.0.2"

resolvers += Resolver.mavenLocal

javacOptions ++= Seq("-source", "11", "-target", "11")

libraryDependencies += "org.apache.flink" % "flink-streaming-java" % "1.15-SNAPSHOT"
libraryDependencies += "org.apache.flink" % "flink-clients" % "1.15-SNAPSHOT"
libraryDependencies += "org.apache.flink" % "flink-table-planner-loader" % "1.15-SNAPSHOT"
libraryDependencies += "org.apache.flink" % "flink-table-common" % "1.15-SNAPSHOT"
libraryDependencies += "org.apache.flink" % "flink-table-api-java" % "1.15-SNAPSHOT"
libraryDependencies += "org.apache.flink" % "flink-table-api-java-bridge" % "1.15-SNAPSHOT"
libraryDependencies += "org.apache.flink" % "flink-table-runtime" % "1.15-SNAPSHOT"
libraryDependencies += "org.apache.flink" % "flink-connector-kafka" % "1.15-SNAPSHOT"

libraryDependencies += "com.github.losizm" %% "little-json" % "9.0.0"
