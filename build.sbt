name := "scala-Practice"

version := "0.1"

scalaVersion := "2.13.8"

val sparkVersion = "3.2.1"

val dependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.hadoop" % "hadoop-aws" % "3.3.1",
  // https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common
  "org.apache.hadoop" % "hadoop-common" % "3.3.1",
    // https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-client
 "org.apache.hadoop" % "hadoop-client" % "3.3.1" % "provided"



)

libraryDependencies ++=dependencies

lazy val root = (project in file("."))
  .settings(
    name :="StructuredStreaming"
  )












