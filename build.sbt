name := "StockStats"

version := "1.0"

scalaVersion := "2.11.11"

resolvers += "Apache repo" at "https://repository.apache.org/content/repositories/releases"

val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
  "com.typesafe.play" %% "play-json" % "2.6.0",
  "org.apache.spark" % "spark-core_2.11" % sparkVersion,
  "org.apache.spark" % "spark-sql_2.11" % sparkVersion ,
  "org.apache.spark" % "spark-streaming_2.11" % sparkVersion,
  "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % sparkVersion,
  "org.apache.kafka" % "kafka_2.11" % "0.8.2.1",
  "org.apache.logging.log4j" % "log4j-to-slf4j" % "2.8.2",
  "org.apache.logging.log4j" % "log4j" % "2.8.2" pomOnly(),
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.6",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)