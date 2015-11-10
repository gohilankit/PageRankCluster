name := "PageRank"

version := "1.0"

scalaVersion := "2.11.6"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.1"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.5.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.6.1"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

