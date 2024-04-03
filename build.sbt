ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.2",
  "org.apache.spark" %% "spark-sql" % "3.4.2",
  "org.apache.spark" %% "spark-mllib" % "3.4.2",
)


lazy val root = (project in file("."))
  .settings(
    name := "sentiment_analysis"
  )
