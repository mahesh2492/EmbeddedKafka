name := "EmbeddedKafka"

version := "1.0"

scalaVersion := "2.11.2"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.2.1",

  //log4j
  "log4j" % "log4j" % "1.2.17",

  //for slfj4
  "org.slf4j" % "slf4j-simple" % "1.7.25",

//scalatest
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",

  //kafka
  "org.apache.kafka" % "kafka-clients" % "0.10.2.0",

  //embeddedkafka
  "net.manub" %% "scalatest-embedded-kafka" % "0.13.1" % "test"
).map(_.exclude("org.slf4j", "log4j-over-slf4j"))