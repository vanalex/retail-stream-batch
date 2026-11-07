# PySpark / Python → Scala 3 + Spark 4 mapping

The patterns below compile against this project's build (Spark 4.3.0, Scala 3.10).

## Contents
- Session and config
- DataFrame API
- JDBC and MariaDB/MySQL
- Kafka (kafka-python → Spark / Kafka clients)
- Structured Streaming sinks
- Redis
- Python idioms

## Session and config

| Python | Scala |
|---|---|
| `SparkSession.builder.appName(..).master("local[2]")` | same, with `SparkSession.builder()` |
| `.config("spark.jars", "jars/…")`, `extraClassPath` | drop: sbt dependencies provide the jars |
| `.config("spark.sql.shuffle.partitions", 2)` | `.config("spark.sql.shuffle.partitions", "2")` (string) |
| `sparkContext.setLogLevel("WARN")` | same; log4j2.properties already sets WARN |
| `findspark`, `PYSPARK_PYTHON`, `HADOOP_HOME`/winutils | drop |

## DataFrame API

| Python | Scala |
|---|---|
| `from pyspark.sql import functions` / `functions.col("x")` | `import org.apache.spark.sql.functions.*` / `col("x")` |
| `df.filter("last_action == 'ShoppingCart'")` | same string, or `col("last_action") === "ShoppingCart"` |
| `df.select(functions.from_json("value", schema).alias("v"))` | `df.select(from_json(col("value"), schema).as("v"))` |
| `StructType([StructField("x", StringType())])` | `StructType(Seq(StructField("x", StringType)))` |
| `TimestampNTZType()` | `TimestampNTZType` |
| `row["window"]["start"]` | `row.getAs[Row]("window").getAs[java.time.LocalDateTime]("start")` (NTZ) / `java.sql.Timestamp` (TZ) |
| `df.show(3)` | `df.show(3)` |
| `@udf` / `functions.udf(f)` | `udf(new UDF1[A, B] { def call(a: A): B = ... }, ReturnType)` |
| `spark.createDataFrame(list_of_tuples, schema)` | `spark.createDataFrame(java.util.List.of(Row(...), ...), schema)` |

## JDBC and MariaDB/MySQL

| Python | Scala |
|---|---|
| `mariadb.connect(user=, password=, host=, port=, database=, autocommit=True)` | `DriverManager.getConnection(cfg.jdbcUrl(db), cfg.mysqlUser, cfg.mysqlPassword)` inside `Using.resource` (JDBC autocommits by default) |
| `cursor.execute(f"INSERT … '{v}'")` | `PreparedStatement` with `?` and `setString`/`setInt`; batch with `addBatch`/`executeBatch` |
| `for a, b in cursor:` | `Iterator.continually(rs).takeWhile(_.next()).map(r => (r.getString(1), r.getInt(2)))` (eager `toList` before closing) |
| Spark `read.format("jdbc").option("url","jdbc:mysql://localhost:3306/db")` | same, with `cfg.jdbcUrl(db)` and `option("driver","com.mysql.cj.jdbc.Driver")` |
| `option("dbtable", "( " + q + " ) as tmp")` + partition bounds | same; or `option("query", q)` when not partitioning (they can't be combined) |
| `df.write.mode("append").format("jdbc")…save()` | same |

## Kafka

| Python (kafka-python) | Scala |
|---|---|
| `KafkaAdminClient` / `NewTopic` | `org.apache.kafka.clients.admin.Admin` / `NewTopic(name, partitions, rf.toShort)` (see `SetupPrerequisites.setupKafkaTopics`) |
| `KafkaProducer(key_serializer=…, value_serializer=…)` | `KafkaProducer[String, String]` with `StringSerializer`; `producer.send(ProducerRecord(topic, key, value))`; close in `Using` |
| `json.dumps(dict)` | build the JSON with Spark (`to_json(struct(...))`) or a small string template; don't add a JSON library just for this |
| `KafkaConsumer(...).poll()` | `KafkaConsumer[String, String]`, `subscribe(java.util.List.of(topic))`, `poll(Duration.ofSeconds(1))` |
| Spark `readStream.format("kafka")` | same options; `kafka.bootstrap.servers = cfg.kafkaBootstrap` |
| Spark Kafka sink | `.writeStream.format("kafka").option("topic", t).option("checkpointLocation", …)`; value column must be named `value` (string/binary) |

## Structured Streaming sinks

```scala
// foreach: one connection per partition/epoch, not per row
.writeStream.foreach(new ForeachWriter[Row]:
  private var conn: java.sql.Connection = scala.compiletime.uninitialized
  def open(partitionId: Long, epochId: Long): Boolean = { conn = ...; true }
  def process(row: Row): Unit = ...
  def close(errorOrNull: Throwable): Unit = if conn != null then conn.close()
).start()

// foreachBatch: parameter types are required in Scala 3
.writeStream.foreachBatch((batch: Dataset[Row], batchId: Long) =>
  batch.write.mode("append").format("jdbc")....save()
).start()
```
Windowed aggregations: `groupBy(window(col("visit_date"), "5 seconds"), col("last_action"))`.
Add `withWatermark` when the output mode is `append`.

## Redis

The notebooks use `zincrby` and `zrevrangebyscore(key, 99999, 0, withscores=True)`.
There's no Redis dependency yet; add one when a notebook first needs it (see SKILL.md step 5).
Open one client per partition inside `ForeachWriter.open`, never per row.

## Python idioms

| Python | Scala |
|---|---|
| `datetime.datetime.today().strftime("%Y-%m-%d")` | `java.time.LocalDate.now().toString` |
| `today - timedelta(2)` | `LocalDate.now().minusDays(2)` |
| `random.randint(a, b)` (inclusive) | `scala.util.Random.between(a, b + 1)` |
| `time.sleep(random.randint(1,3))` | `Thread.sleep(Random.between(1, 4) * 1000L)` |
| `while True:` monitor loop | loop with a `--iterations N` / duration argument so it can end (and be tested) |
| `os.makedirs(p, exist_ok=True)` | `Files.createDirectories(Paths.get(p))` |
