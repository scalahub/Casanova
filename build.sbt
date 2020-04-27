name := "Casanova"

version := "0.1"

scalaVersion := "2.13.2"

val CassandraDriverVersion = "4.5.0"

libraryDependencies ++= Seq(
  "org.scala-lang.modules" %% "scala-java8-compat" % "0.9.1",
  "org.scala-lang.modules" %% "scala-collection-compat" % "2.1.4",
  "com.datastax.oss" % "java-driver-core" % CassandraDriverVersion,
  "com.datastax.oss" % "java-driver-query-builder" % CassandraDriverVersion,
  "org.cassandraunit" % "cassandra-unit" % "4.3.1.0" % Test,
  "org.scalatestplus.play" %% "scalatestplus-play" % "4.0.3" % Test,
  "org.mockito" % "mockito-core" % "2.18.3" % Test
)
