// Your sbt build file. Guides on how to write one can be found at
// http://www.scala-sbt.org/0.13/docs/index.html

// Project name
name := """spark-dataframe-utils"""

// Don't forget to set the version
version := "0.1.4-SNAPSHOT"

// Org
organization := "com.paypal.risk.spark"

// All Spark Packages need a license
licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))

// scala version to be used
scalaVersion := "2.11.6"
//scalaVersion := "2.10.4"

// force scalaVersion
//ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

// spark version to be used
val sparkVersion = "1.5.0"

// Needed as SBT's classloader doesn't work well with Spark
fork := true

// BUG: unfortunately, it's not supported right now
fork in console := true

// Java version
javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

// add a JVM option to use when forking a JVM for 'run'
javaOptions ++= Seq("-Xmx2G")

// append -deprecation to the options passed to the Scala compiler
scalacOptions ++= Seq("-deprecation", "-unchecked")

// Use local repositories by default
resolvers := Seq(
  Resolver.defaultLocal,
  Resolver.mavenLocal,
  // make sure default maven local repository is added... Resolver.mavenLocal has bugs.
  "Local Maven Repository" at Path.userHome.asFile.toURI.toURL + "/.m2/repository",
  // For Typesafe goodies, if not available through maven
  // "Typesafe" at "http://repo.typesafe.com/typesafe/releases",
  // For Spark development versions, if you don't want to build spark yourself
  "PayPal Nexus releases" at "http://nexus.paypal.com/nexus/content/repositories/releases",
  "PayPal Nexus snapshots" at "http://nexus.paypal.com/nexus/content/repositories/snapshots"
) ++ resolvers.value ++ Seq(
  "Apache Staging" at "https://repository.apache.org/content/repositories/staging/"
)


// Enable cached resolution
updateOptions := updateOptions.value.withCachedResolution(true)


/// Dependencies

// copy all dependencies into lib_managed/
retrieveManaged := true

// scala modules (should be included by spark, just an exmaple)
//libraryDependencies ++= Seq(
//  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
//  "org.scala-lang" % "scala-compiler" % scalaVersion.value
//  )

val sparkDependencyScope = "provided"

// spark modules (should be included by spark-sql, just an example)
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % sparkDependencyScope,
  "org.apache.spark" %% "spark-sql" % sparkVersion % sparkDependencyScope,
  "org.apache.spark" %% "spark-hive" % sparkVersion % sparkDependencyScope
)

// logging
//libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0"

// joda datetime library
libraryDependencies ++= Seq(
  "joda-time" % "joda-time" % "2.5",
  "org.joda" % "joda-convert" % "1.7"
)

// testing
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.12.2" % "test"


/// Compiler plugins

// linter: static analysis for scala
resolvers += "Linter Repository" at "https://hairyfotr.github.io/linteRepo/releases"

addCompilerPlugin("com.foursquare.lint" %% "linter" % "0.1.8")


/// console

// define the statements initially evaluated when entering 'console', 'consoleQuick', or 'consoleProject'
// but still keep the console settings in the sbt-spark-package plugin

// If you want to use yarn-client for spark cluster mode, override the environment variable
// SPARK_MODE=yarn-client <cmd>
val sparkMode = sys.env.getOrElse("SPARK_MODE", "local[2]")


initialCommands in console :=
  s"""
    |import org.apache.spark.SparkConf
    |import org.apache.spark.SparkContext
    |import org.apache.spark.SparkContext._
    |
    |@transient val sc = new SparkContext(
    |  new SparkConf()
    |    .setMaster("$sparkMode")
    |    .setAppName("Console test"))
    |implicit def sparkContext = sc
    |import sc._
    |
    |@transient val sqlc = new org.apache.spark.sql.SQLContext(sc)
    |implicit def sqlContext = sqlc
    |import sqlc._
    |
    |def time[T](f: => T): T = {
    |  import System.{currentTimeMillis => now}
    |  val start = now
    |  try { f } finally { println("Elapsed: " + (now - start)/1000.0 + " s") }
    |}
    |
    |import org.apache.spark.sql.ext._
    |import org.apache.spark.sql.ext.dataframe.Implicits._
    |
    |""".stripMargin

cleanupCommands in console :=
  s"""
     |sc.stop()
   """.stripMargin


/// scaladoc
scalacOptions in (Compile,doc) ++= Seq("-groups", "-implicits",
  // NOTE: remember to change the JVM path that works on your system.
  // Current setting should work for JDK7 on OSX and Linux (Ubuntu)
  "-doc-external-doc:/Library/Java/JavaVirtualMachines/jdk1.7.0_60.jdk/Contents/Home/jre/lib/rt.jar#http://docs.oracle.com/javase/7/docs/api",
  "-doc-external-doc:/usr/lib/jvm/java-7-openjdk-amd64/jre/lib/rt.jar#http://docs.oracle.com/javase/7/docs/api"
  )

autoAPIMappings := true


/// static analysis and style check
//scapegoatIgnoredFiles := Seq(".*/UdfUtils.scala", ".*/MapRDD.scala")


/// publishing
publishMavenStyle := true

publishTo := {
  val nexus = "http://nexus.paypal.com/nexus/content/repositories/"
  if (version.value.trim.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "snapshots")
  else
    Some("releases"  at nexus + "releases")
}

// http://www.scala-sbt.org/0.13.5/docs/Detailed-Topics/Publishing.html#credentials
//credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

pomExtra := (
  <url>https://github.paypal.com/Risk-DataMartInfra/spark-dataframe-utils</url>
    <scm>
      <url>git@github.paypal.com:Risk-DataMartInfra/spark-dataframe-utils.git</url>
      <connection>scm:git@github.paypal.com:Risk-DataMartInfra/spark-dataframe-utils.git</connection>
    </scm>
    <developers>
      <developer>
        <id>jianshuang</id>
        <name>Jianshi Huang</name>
        <url>https://github.paypal.com/jianshuang</url>
      </developer>
    </developers>)

/// Assembly
// skip test in assembly
test in assembly := {}

// do not include scala libraries
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

// do not include scapegoat jars
assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp filter { cp =>
    cp.data.getName.startsWith("scalac-scapegoat-plugin") || cp.data.getName.startsWith("scaldi")
  }
}

/// sbt-pack
// Automatically find def main(args:Array[String]) methods from classpath
packAutoSettings

// If you need to specify main classes manually, use packSettings and packMain
//packSettings

// [Optional] Creating `hello` command that calls org.mydomain.Hello#main(Array[String])
//packMain := Map("hello" -> "org.paypal.risk.grs.Hello")

packExcludeJars := Seq(
  "scala-.*\\.jar",
  "scalac-scapegoat-plugin.*.jar",
  "scaldi_.*.jar"
)
