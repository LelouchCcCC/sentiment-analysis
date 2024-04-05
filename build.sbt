ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

resolvers += "Akka library repository".at("https://repo.akka.io/maven")

val sparkCsvVersion = "1.4.0"
val coreNlpVersion = "3.6.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.2",
  "org.apache.spark" %% "spark-sql" % "3.4.2",
  "org.apache.spark" %% "spark-mllib" % "3.4.2",
  "org.xerial" % "sqlite-jdbc" % "3.39.3.0",
  "edu.stanford.nlp" % "stanford-corenlp" % coreNlpVersion,
  "edu.stanford.nlp" % "stanford-corenlp" % coreNlpVersion classifier "models",
  // https://mvnrepository.com/artifact/com.typesafe.akka/akka-http-core
  "com.typesafe.akka" %% "akka-http-core" % "10.6.1",
  "com.typesafe.akka" %% "akka-stream" % "2.9.2",
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.6.1",
"com.typesafe.akka" %% "akka-http-testkit" % "10.6.1" % "Test",
"com.typesafe.akka" %% "akka-stream-testkit" % "2.9.2" % "Test",
)


lazy val root = (project in file("."))
  .settings(
    name := "sentiment_analysis"
  )
