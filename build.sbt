resolvers in ThisBuild ++= Seq(
    "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
    Resolver.mavenLocal
)

name := "flink-scala-dufry-bam"

version := "0.1-SNAPSHOT"

organization := "org.example"

scalaVersion in ThisBuild := "2.11.7"

val flinkVersion = "1.4.0"


val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided")

libraryDependencies += "org.apache.flink" %% "flink-connector-kafka-0.11" % "1.4.0"
// https://mvnrepository.com/artifact/org.apache.flink/flink-cep-scala
libraryDependencies += "org.apache.flink" %% "flink-cep-scala" % "1.4.1"

//libraryDependencies += "org.apache.flink" %% "flink-cep-scala" % "1.4.0"
//libraryDependencies += "org.apache.flink" %% "flink-connector-elasticsearch5" % "1.4.0"
// Parece ser que la 1.4.1 de flink-connector-elasticsearch5, resuelve "Multiple Elasticsearch sinks not working in Flink"
libraryDependencies += "org.apache.flink" %% "flink-connector-elasticsearch5" % "1.4.1"


//Parece que la versión 1.4.1 lo incluye
//libraryDependencies += "org.elasticsearch.client" % "transport" % "5.1.2"
//libraryDependencies += "org.elasticsearch" % "elasticsearch" % "5.1.2"

libraryDependencies += "org.apache.lucene" % "lucene-core" % "6.3.0"

libraryDependencies += "org.apache.logging.log4j" % "log4j-to-slf4j" % "2.7"
libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.7"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3" % Test

libraryDependencies += "org.apache.flink" % "flink-shaded-jackson" % "2.7.9-2.0"

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= flinkDependencies
  )

mainClass in assembly := Some("org.example.KafkaStreamDufryBAM")

// make run command include the provided dependencies
run in Compile := Defaults.runTask(fullClasspath in Compile,
                                   mainClass in (Compile, run),
                                   runner in (Compile,run)
                                  ).evaluated

// exclude Scala library from assembly
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x =>
	val oldStrategy = (assemblyMergeStrategy in assembly).value
	oldStrategy(x)
	}