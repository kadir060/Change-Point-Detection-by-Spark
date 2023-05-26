//ThisBuild / version := "0.1.0-SNAPSHOT"
//
//ThisBuild / scalaVersion := "2.13.10"
//
//ThisBuild / libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.2"
//lazy val root = (project in file("."))
//  .settings(
//    name := "Deneme"
//  )

name := "AnomalyDetection"

version := "0.1"

scalaVersion := "2.12.15"

val sparkVersion = "3.1.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-mllib-evaluation" % sparkVersion
)