name := "advanced-analytics"

version := "0.2"

scalaVersion := "2.10.5"


// spark and scala
libraryDependencies += "com.github.scopt" %% "scopt" % "3.2.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.1"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.3.1"

// NLP
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.3.1"

libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.3.1" classifier "models"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.4.0"

libraryDependencies += "com.cloudera.datascience" % "common" % "1.0.0"

// GraphX
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.3.1"

// Time-based data
libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.0.0"

// Geographic data
libraryDependencies += "io.spray" % "spray-json_2.10" % "1.3.2"

libraryDependencies += "com.esri.geometry" % "esri-geometry-api" % "1.2.1"

// The visualizer from chapter 9
libraryDependencies += "org.scalanlp" % "breeze-viz_2.10" % "0.3"

// The BDG dependencies from chapter 10
libraryDependencies += "org.bdgenomics.bdg-formats" % "bdg-formats" % "0.4.0"

libraryDependencies += "org.bdgenomics.adam" % "adam-core" % "0.16.0"

libraryDependencies += "org.bdgenomics.bdg-utils" % "bdg-utils-parquet" % "0.1.1"

resolvers += Resolver.mavenLocal
