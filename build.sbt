
ThisBuild / scalaVersion := "2.12.18"


libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.18" % Test,
  "org.apache.spark" %% "spark-core" % "3.5.1",
  "org.apache.spark" %% "spark-sql"  % "3.5.1",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.1",
  "io.delta" %% "delta-spark" % "3.1.0",
  "org.apache.kafka" % "kafka-clients" % "3.7.0",
  "org.apache.kafka" % "kafka-streams" % "3.6.1"


)
