import _root_.sbt.Keys._

name := "fun-spark"

version := "1.0.0"

scalaVersion := "2.10.5"

val hbaseVersion = "0.98.8-hadoop2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.1",
  "org.apache.spark" % "spark-streaming_2.10" % "1.6.1",
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.1",
  "org.apache.hbase" % "hbase-client" % hbaseVersion,
  "org.apache.hbase" % "hbase-common" % hbaseVersion,
  "org.apache.hbase" % "hbase-protocol" % hbaseVersion,
  "org.apache.hbase" % "hbase-server" % hbaseVersion excludeAll ExclusionRule(organization = "org.mortbay.jetty"),
  "mysql" % "mysql-connector-java" % "5.1.18"
)

javacOptions ++= Seq("-encoding", "utf-8")

resolvers += "sbt-pack repo" at " http://repo1.maven.org/maven2/org/xerial/sbt/"

packSettings

packMain := Map(
  "start" -> "com.bi.fun.spark.PushReachStat"
)
