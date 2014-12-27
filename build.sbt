import sbt._
import sbt.Keys._

name := "etherd"

version := "1.0"

scalaVersion := "2.10.3"

lazy val root = project.in(file(".")).aggregate(core)

lazy val core = project.settings(
      libraryDependencies += "org.fluentd" % "fluent-logger" % "0.2.10",
      libraryDependencies += "net.sf.jopt-simple" % "jopt-simple" % "3.2",
      libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.7",
      libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.7",
      libraryDependencies += "com.101tec" % "zkclient" % "0.4",
      libraryDependencies += "com.yammer.metrics" % "metrics-core" % "2.2.0",
      libraryDependencies += "com.yammer.metrics" % "metrics-annotation" % "2.2.0",
      libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.2.0",
      libraryDependencies += "org.apache.hadoop" % "hadoop-yarn-api" % "2.2.0",
      libraryDependencies += "org.apache.hadoop" % "hadoop-yarn-client" % "2.2.0",
      libraryDependencies += "org.apache.hadoop" % "hadoop-yarn-common" % "2.2.0",
      libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.2.2"
 )

