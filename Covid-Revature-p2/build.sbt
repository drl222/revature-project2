name := "Covid-Revature-p2"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.3.1"

libraryDependencies += "org.scalanlp" %% "breeze" % "1.2"
//libraryDependencies += "org.scalanlp" %% "breeze-viz" % "1.2"
// The visualization library is distributed separately as well.
// It depends on LGPL code
