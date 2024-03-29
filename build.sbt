ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "BogDataSpark2324"
  )
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.2"
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.9.0"
