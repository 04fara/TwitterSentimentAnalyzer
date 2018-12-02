name := "spark2demo"

version := "0.1"

scalaVersion := "2.11.6"

val sparkVersion = "2.4.0"

libraryDependencies ++= Seq(
  "edu.stanford.nlp" % "stanford-corenlp" % "3.9.2",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.9.2" classifier "models",
  "com.github.catalystcode" %% "streaming-rss-html" % "1.0.2",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion
)
