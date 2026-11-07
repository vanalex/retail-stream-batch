val scala3Version = "3.10.0"

// Apache Spark does not publish Scala 3 (_3) artifacts. The newest release, 4.3.0,
// is built for Scala 2.13.18. Scala 3 can use Scala 2.13 libraries directly, so the
// Spark modules are pulled with CrossVersion.for3Use2_13.
// SparkCompatibilitySuite checks that this setup works.
val sparkVersion = "4.3.0"

// JVM flags Spark 4 needs on JDK 17+. spark-submit normally adds these
// (org.apache.spark.launcher.JavaModuleOptions); forked sbt JVMs need them set here.
val sparkJavaOptions = Seq(
  "-XX:+IgnoreUnrecognizedVMOptions",
  "--add-modules=jdk.incubator.vector",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "-Djdk.reflect.useDirectMethodHandle=false",
  "-Dio.netty.tryReflectionSetAccessible=true"
)

lazy val root = project
  .in(file("."))
  .settings(
    name := "retail-stream-batch",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    libraryDependencies ++= Seq(
      ("org.apache.spark" %% "spark-sql"            % sparkVersion).cross(CrossVersion.for3Use2_13),
      ("org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion).cross(CrossVersion.for3Use2_13),
      "com.mysql"          % "mysql-connector-j"    % "26.10.0",
      "org.scalameta"     %% "munit"                % "1.3.6" % Test
    ),

    // Spark must run in a forked JVM (module flags, classloader isolation)
    run / fork := true,
    Test / fork := true,
    run / javaOptions ++= sparkJavaOptions,
    Test / javaOptions ++= sparkJavaOptions ++ Seq("-Xmx2g"),
    Test / parallelExecution := false
  )
