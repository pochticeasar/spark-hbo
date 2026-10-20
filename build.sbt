ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "spark-hbo",
    version := "0.1",
    // Версия должна совпадать с той, на которой запускаешь (pip install pyspark==3.5.3)
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.3" % "provided"
  )
