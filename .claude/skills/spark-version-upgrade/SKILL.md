---
name: spark-version-upgrade
description: Check for and apply upgrades to Spark, Scala, the Kafka connector, the MySQL driver or munit in build.sbt, and confirm Scala 3 compatibility. Use when the user asks about newer Spark/Scala versions, whether Spark supports Scala 3 yet, dependency upgrades, or after a version bump breaks the build.
---

# Spark / Scala version upgrades

Current setup (see the comments in `build.sbt`): Spark publishes no Scala 3 (`_3`) artifacts, so the
Spark modules use their `_2.13` builds via `.cross(CrossVersion.for3Use2_13)`.
`src/test/scala/SparkCompatibilitySuite.scala` is the compatibility gate.

## 1. Find the latest versions (don't rely on memory)

```bash
meta() { curl -s "https://repo1.maven.org/maven2/$1/maven-metadata.xml" | grep -E '<release>'; }
meta org/apache/spark/spark-core_3            # empty = still no Scala 3 build
meta org/apache/spark/spark-sql_2.13
meta org/apache/spark/spark-sql-kafka-0-10_2.13
meta org/scala-lang/scala3-library_3          # skip -RC releases
meta com/mysql/mysql-connector-j
meta org/scalameta/munit_3
```
Find the Scala 2.13 and Java versions a Spark release is built with:
`curl -s https://repo1.maven.org/maven2/org/apache/spark/spark-parent_2.13/<v>/spark-parent_2.13-<v>.pom | grep -E '<scala.version>|<java.version>'`

## 2. Decide

- **If `spark-core_3` exists:** switch to plain `%%` for Spark modules, drop `for3Use2_13`, and
  revisit the Scala 3 limits in the `spark-scala-job` skill (encoders, UDFs). The upgrade gets
  bigger, so confirm the plan with the user first.
- Keep `spark-sql` and `spark-sql-kafka-0-10` on the **same** version.
- Skip previews and RCs unless the user asks for them.
- A new Spark major or minor version can change the JDK flags it needs. Compare `sparkJavaOptions` in `build.sbt` with
  `org.apache.spark.launcher.JavaModuleOptions` in that release.

## 3. Apply and verify

1. Edit the version vals in `build.sbt` and update the version comment above `sparkVersion`.
2. Update the hard-coded version assertions in `SparkCompatibilitySuite` ("Spark runtime is …").
3. `sbt testFull`. All suites must pass, including the integration suite if the stack is up.
4. Run one real job against the stack: `sbt "runMain retail.setup.runSetup"`.
5. Report the old → new versions and anything that needed changing.

## Troubleshooting

- `conflicting cross-version suffixes` (`_2.13` vs `_3`): some library pulled a Scala 3 build of a module
  that Spark brings as 2.13 (e.g. scala-xml, scala-parallel-collections). Exclude one side:
  `.excludeAll(ExclusionRule("org.scala-lang.modules", "scala-xml_2.13"))`. Check with `sbt evicted` / `sbt "whatDependsOn <org> <name> <ver>"`.
- `NoSuchMethodError` or `IllegalAccessError` at runtime: usually a missing `--add-opens`, or Jackson/Netty
  version drift. Run `sbt dependencyTree` and compare with the Spark pom.
- A helper library (e.g. `spark-scala3-encoders`) targets an older Spark: check its pom's
  `spark-sql_2.13` version before adding it. 0.3.2 targets Spark 3.5.5 and doesn't fit 4.x.
