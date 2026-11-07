---
name: spark-scala-job
description: Conventions for writing, running and testing Spark jobs in this Scala 3 + Spark 4.3 project (batch, Structured Streaming, JDBC to MySQL, Kafka/Redpanda, Redis). Use when creating or changing any code under src/main/scala/retail, adding a Spark job or test, or when Scala 3 code fails to compile against the Spark API.
---

# Writing Spark jobs here

Stack: Scala 3.10, Spark 4.3.0 (Scala 2.13 artifacts via `CrossVersion.for3Use2_13`), sbt 2,
munit, JDK 17+. Read `build.sbt` before adding dependencies.

## Layout

```
src/main/scala/retail/
  config/InfraConfig.scala   connection settings (env-overridable); always use it, never hard-code hosts
  setup/                     one-off setup (runSetup)
  batch/                     batch jobs (stock upload / aggregation)
  streaming/                 Structured Streaming jobs
src/test/scala/              munit suites, one per job
```

One job = one file with a `@main def runXxx()` entry point plus an `object Xxx` holding
the testable functions. **The `@main` name must not differ only in case from the object name**:
both compile to a class, and on macOS's case-insensitive filesystem one overwrites the other
(`ClassNotFoundException` at `runMain`). Run with `sbt "runMain retail.<pkg>.runXxx"`.

## SparkSession

Use `retail.config.Spark`. Don't call the builder yourself:
- `@main` entry points: `Spark.withSession("StockUploaderJob") { spark => ... }` (stops the session afterwards).
- Job functions take `spark: SparkSession` as a parameter, so tests can pass their shared session.
  Never call `withSession` inside a function that tests call: `getOrCreate` would hand back the
  test's session, and `withSession` would then stop it.
- Tests: `Spark.localSession("XxxSuite")` in `beforeAll`, `stop()` in `afterAll`.
Don't add `spark.jars` / `extraClassPath`: sbt already puts the Kafka and MySQL jars on the classpath.
JVM `--add-opens` flags are already in `build.sbt` (`fork := true`), but only sbt runs get them.
IDE runs need the shared configs in `.run/` (add one for each new `@main`, with VM options copied from
`sparkJavaOptions`). Without the flags, `Spark.localSession` throws an `IllegalStateException`
explaining this, instead of failing mid-job with `IllegalAccessException ... sun.util.calendar.ZoneInfo`.

## Scala 3 rules (they all compile today; keep to them)

- **No `import spark.implicits.*` for typed data.** It needs Scala 2 `TypeTag`s. Use the
  DataFrame/SQL API, or pass encoders with `using`: `ds.map(_.length)(using Encoders.scalaInt)`.
  Case-class Datasets (`Encoders.product`) don't work.
- **UDFs** go through the Java API:
  `udf(new UDF1[String, String] { def call(s: String) = s.toUpperCase }, StringType)`.
- **foreachBatch** needs explicit parameter types:
  `.foreachBatch((batch: Dataset[Row], batchId: Long) => ...)`.
- **foreach sink**: `new ForeachWriter[Row]: def open(...)...; def process(row: Row)...; def close(...)...`.
  Open connections in `open`, not per row.
- Collect results with `Row` getters (`r.getString(0)`, `r.getLong(1)`). `COUNT`/`SUM` of INT come back as `Long`.

## JDBC (MySQL)

```scala
spark.read.format("jdbc")
  .options(cfg.jdbcOptions("warehouse_stock"))   // driver, url (allowPublicKeyRetrieval), user, password
  .option("query", sql)        // or dbtable + partitionColumn/lowerBound/upperBound/numPartitions
  .load()
```
For small lookups and writes, use `Using.resource(cfg.connect(db))` with `PreparedStatement`
(see `StockGenerator.replaceStock`, `StockUploader.idBounds`). Use root only for DDL and grants
(`SetupPrerequisites.setupMySql`); jobs connect as `spark`.

Make re-runs safe: replace the slice you write instead of appending. For Parquet, use
`mode("overwrite")` + `option("partitionOverwriteMode", "dynamic")` (see `StockUploader.writeRaw`).
For MySQL, delete the dates or keys being rewritten, then append (see `GlobalAggregator.save`).

## Streaming

Kafka source/sink: `format("kafka")`, `kafka.bootstrap.servers = cfg.kafkaBootstrap`.
Always set `checkpointLocation` under `target/checkpoints/<job>` so restarts are reproducible.
Topics: see `retail.setup.Prerequisites.topics`.

## Tests

- Unit-test the transformations on small in-memory DataFrames (`spark.createDataFrame(java.util.List.of(Row(...)), schema)`),
  as `SparkCompatibilitySuite` does. Keep one SparkSession per suite (`beforeAll`/`afterAll`).
- Tests that need the Docker stack must call `assume(...)` when MySQL or Kafka can't be reached,
  as `SetupPrerequisitesSuite.assumeStack` does, so `sbt test` still passes without Docker.
- sbt 2's `test` is incremental (it only re-runs suites affected by changes). Use `sbt testFull` before calling work done,
  and `sbt "testOnly XxxSuite"` while iterating.
- Spark logging is set to WARN via `src/{main,test}/resources/log4j2.properties`.

## Done checklist

1. `sbt testFull` is green and shows the test counts.
2. If the job touches infra, run it once against the stack (see the `local-infra` skill).
3. Changes to a database or topic go into **both** `infra/mysql/init/01-schema.sql` / compose `redpanda-init`
   **and** `retail.setup.Prerequisites`.
