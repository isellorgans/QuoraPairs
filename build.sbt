name := "Dataframes"

version := "1.0"

scalaVersion := "2.11.8"


resolvers += "Typesafe Releases" at 
"http://repo.typesafe.com/typesafe/releases"

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

//alternative resolver as the above didn't seem to work 0725
resolvers += Resolver.url("SparkPackages", url("https://dl.bintray.com/spark-packages/maven/"))






libraryDependencies += "graphframes" % "graphframes" % "0.5.0-spark2.1-s_2.11"


libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.0" % "provided"


libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.0" % "provided"


libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.2.0" % "provided"


libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.2.0" % "provided"


libraryDependencies += "org.scala-lang.modules" % "scala-xml_2.11" % "1.0.6"

libraryDependencies += "org.apache.spark" % "spark-graphx_2.11" % "2.2.0" % "provided"

libraryDependencies += "com.typesafe.akka" % "akka-actor_2.11" % "2.4.17"

libraryDependencies += "org.scalanlp" % "breeze_2.11" % "0.13.1"

libraryDependencies += "org.scalanlp" % "breeze-viz_2.11" % "0.13.1"

libraryDependencies += "org.scalanlp" % "breeze-natives_2.11" % "0.13.1"


fork in run := true