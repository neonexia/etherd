import sbt._
import sbt.Keys._

name := "etherd"

version := "1.0"

scalaVersion := "2.11.4"

lazy val root = project.in(file(".")).aggregate(core).settings (
	libraryDependencies += "org.apache.kafka" % "kafka_2.10" % "0.8.2.1",
	libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.8.2.1",
	libraryDependencies += "com.yammer.metrics" % "metrics-core" % "2.2.0",
	libraryDependencies += "com.yammer.metrics" % "metrics-annotation" % "2.2.0"
)

lazy val core = project.settings(
  libraryDependencies += "org.fluentd" % "fluent-logger" % "0.2.10",
  libraryDependencies += "net.sf.jopt-simple" % "jopt-simple" % "3.2",
  libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.7",
  libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.2",
  libraryDependencies += "ch.qos.logback" % "logback-core" % "1.1.2",
  libraryDependencies += "com.101tec" % "zkclient" % "0.4",
  libraryDependencies += "org.apache.kafka" % "kafka_2.10" % "0.8.2.1",
  libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.8.2.1",
  libraryDependencies += "com.yammer.metrics" % "metrics-core" % "2.2.0",
  libraryDependencies += "com.yammer.metrics" % "metrics-annotation" % "2.2.0",
  libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.2.0",
  libraryDependencies += "org.apache.hadoop" % "hadoop-yarn-api" % "2.2.0",
  libraryDependencies += "org.apache.hadoop" % "hadoop-yarn-client" % "2.2.0",
  libraryDependencies += "org.apache.hadoop" % "hadoop-yarn-common" % "2.2.0",
  libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.2",
  libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.3.9",
  libraryDependencies += "com.typesafe.akka" %% "akka-remote" % "2.3.9",
  libraryDependencies += "com.typesafe" % "config" % "1.2.1",
  libraryDependencies += "com.typesafe.akka" %% "akka-slf4j" % "2.3.9",

  parallelExecution in Test := false
)

