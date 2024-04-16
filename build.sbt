name := "spark-taxi"

version := "0.0.1"

scalaVersion := "2.13.13"

val sparkVersion = "3.5.1"
val postgresVersion = "42.6.0"
val log4jVersion = "2.23.1"


ThisBuild / assembly / mainClass := Some("eu.karnicki.Taxi")
ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion, //% "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion, //% "provided",
  // logging
  "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-core" % log4jVersion,
  // postgres for DB connectivity
  "org.postgresql" % "postgresql" % postgresVersion
)