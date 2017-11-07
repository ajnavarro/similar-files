import sbt.Keys.libraryDependencies

name := "similar-files"

version := "0.1"

scalaVersion := "2.11.10"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % Test
libraryDependencies += "tech.sourced" % "engine" % "0.1.5" % Compile
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0" % Compile
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.2.0" % Compile
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.2" % Compile