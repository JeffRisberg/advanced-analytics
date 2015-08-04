name := "advanced-analytics"

version := "0.2"

scalaVersion := "2.10.5"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.2.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.1"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.3.1"

libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.3.1"

libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.3.1" classifier "models"

resolvers += Resolver.sonatypeRepo("public")