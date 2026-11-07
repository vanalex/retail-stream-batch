---
name: port-notebook
description: Port a notebook from the LinkedIn Learning course "Apache Spark Essential Training" (Ex_Files_Apache_Spark_Essential_Training, PySpark + mariadb/kafka-python/redis-py) into a Scala 3 + Spark 4 job in this project. Use when the user names a notebook such as code_03_XX or code_04_04, or asks to migrate/convert/port course or PySpark code to Scala.
argument-hint: "[notebook name, e.g. code_04_04]"
---

# Port a course notebook to Scala

Source folder (override with `$COURSE_DIR` if it moved):
`/Users/alex/pythonws/Ex_Files_Apache_Spark_Essential_Training/Exercise Files`

## Status

| Notebook | What it does | Target | Status |
|---|---|---|---|
| code_00_03 Setup_Prerequisites | DBs, tables, grants, topics, raw_data dir | `retail.setup.runSetup` | done |
| code_03_XX Batch_Data_Engg | generate warehouse stock → Parquet upload → global aggregation to MySQL | `retail.batch.runStockPipeline` (+ `runStockGenerator`/`runStockUploader`/`runGlobalAggregator`) | done |
| code_04_03 Generate a visits stream | Kafka producer of random visit JSON | `retail.streaming` (producer) | todo |
| code_04_04 Build_a_streaming_analytics_job | Kafka → abandoned carts topic, Redis zset, windowed stats to MySQL | `retail.streaming` | todo |
| code_04_05 Execute_the_pipeline | console monitor of Kafka + Redis + MySQL | `retail.streaming` (monitor) | todo |
| code_06_03 Extracting_long_last_actions | JDBC read of long visits → Kafka | `retail.batch` | todo |
| code_06_04 scorecard_for_last_action | Kafka → Redis counts | `retail.streaming` | todo |

Update this table when a port lands.

## Workflow

1. **Read the notebook in full.** Don't port from memory or summaries:
   `python3 -I .claude/skills/port-notebook/scripts/dump_notebook.py "<notebook path>"`
   (prints every cell untruncated; `-I` keeps it from importing files in the course folder).
2. **Map each cell** to a function using [reference/pyspark-to-scala.md](reference/pyspark-to-scala.md).
   Leave out notebook-only cells (pip installs, env checks, Windows winutils) and list them in the summary.
3. **Follow the `spark-scala-job` skill** for layout, the SparkSession, Scala 3 rules and tests.
4. **Keep behaviour, fix the hazards.** Same tables, topics, keys, window sizes and outputs.
   Do fix these and mention them to the user:
   - SQL built with f-strings → `PreparedStatement` / Spark JDBC options.
   - A DB/Redis connection opened per row → open once per partition (`ForeachWriter.open`) or per batch.
   - Streams with no checkpoint → `target/checkpoints/<job>`.
   - Steps that aren't safe to re-run → make them safe to re-run, or say they aren't.
5. **Add a Redis client only when a notebook needs Redis.** Pick a maintained JVM client
   (check Maven Central for the latest release), add it to `build.sbt`, and note it in the summary.
6. **Test:** unit tests for the transformations, plus an integration test that skips without the
   stack. Then run the job once against the stack and show its output.
7. Update the status table above.

## Done means

- `sbt testFull` green, with counts reported.
- The job ran against the live stack (or you say why it couldn't).
- The summary lists any behaviour changes and the notebook cells that were left out.
