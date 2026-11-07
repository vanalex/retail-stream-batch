---
name: inspect-pipeline-data
description: Look at the data the retail pipelines read and write - MySQL tables (warehouse_stock, global_stock, website_stats), Kafka/Redpanda topics, Redis scoreboards, and the raw_data Parquet folder. Use when checking a job's output, debugging "no data" or wrong results, or when the user asks what is in a table, topic or key.
allowed-tools:
  - Bash(docker exec mysql mysql *)
  - Bash(docker exec redpanda rpk topic list*)
  - Bash(docker exec redpanda rpk topic describe *)
  - Bash(docker exec redpanda rpk topic consume *)
  - Bash(docker exec redis redis-cli ZREVRANGE *)
  - Bash(docker exec redis redis-cli --scan*)
  - Bash(find raw_data *)
---

# Inspect pipeline data

These commands are read-only. They need the stack running (see `local-infra`). Use container
CLIs instead of writing Scala for a quick look.

## MySQL

```bash
docker exec mysql mysql -uspark -pspark -t -e "<SQL>" 2>/dev/null
```
(`2>/dev/null` hides the password-on-command-line warning; leave it off if the command fails.)

| Table | Written by | Useful query |
|---|---|---|
| `warehouse_stock.item_stock` | stock generator (batch) | `SELECT WAREHOUSE_ID, DATE(STOCK_DATE) d, COUNT(*) FROM warehouse_stock.item_stock GROUP BY 1,2` |
| `global_stock.item_stock` | global aggregation (batch) | `SELECT * FROM global_stock.item_stock WHERE ITEM_NAME='Pencil Box' ORDER BY STOCK_DATE` |
| `website_stats.visit_stats` | streaming windowed stats | `SELECT LAST_ACTION, SUM(DURATION) FROM website_stats.visit_stats GROUP BY 1` |

Batch check from the course: global `OPENING_STOCK`/`RECEIPTS`/`ISSUES` for an item and date
should equal the sum of the three warehouse rows for that item and date.

## Kafka / Redpanda

```bash
docker exec redpanda rpk topic list
docker exec redpanda rpk topic describe <topic> -p          # offsets per partition (HIGH-WATERMARK = message count)
docker exec redpanda rpk topic consume <topic> -o start -n 10 -f '%k  %v\n'
```
`consume` without `-n` blocks forever, so always pass `-n`, or use the bounded form above.
If the topic holds fewer than `-n` messages, `consume` waits for more, so check the high watermark first.
Visual browser: http://localhost:8080.

| Topic | Content |
|---|---|
| `spark.streaming.website.visits` | JSON `{country, last_action, visit_date, duration}`, key = country |
| `spark.streaming.carts.abandoned` | CSV `country,last_action,visit_date,duration` |
| `spark.exercise.lastaction.long` | last_action strings for visits longer than 15s |

## Redis

```bash
docker exec redis redis-cli ZREVRANGE last-action-stats 0 -1 WITHSCORES       # duration per country
docker exec redis redis-cli ZREVRANGE long-last-action-stats 0 -1 WITHSCORES  # count per long last_action
docker exec redis redis-cli --scan --count 100                                 # list keys (never KEYS *)
```

## raw_data (Parquet)

`find raw_data -name '*.parquet' | head` shows the partitions (`STOCK_DATE=…/WAREHOUSE_ID=…`).
To read the contents, use Spark (`spark.read.parquet("raw_data")`) in a test or scratch job.

## Clearing data

Deleting rows, topics or keys is destructive. Only do it when the user asks, and name what
will go. Examples: `DELETE FROM <table>`, `rpk topic delete <t>` (then re-run
`sbt "runMain retail.setup.runSetup"` to recreate the topic), `redis-cli DEL <key>`.
