---
name: local-infra
description: Start, stop, reset, or troubleshoot the local Docker stack (MySQL 8.4, Redpanda/Kafka, Redis, Redpanda Console) defined in docker-compose.yml. Use when the user asks to run/start/restart the infra, when a job or test fails to connect to MySQL, Kafka or Redis, or when there are "Access denied", "Connection refused" or port-in-use errors.
---

# Local infrastructure

Defined in `docker-compose.yml`. The MySQL schema is created on first start by
`infra/mysql/init/01-schema.sql`; topics are created by the one-shot `redpanda-init` service.

| Service | Host address | Inside the compose network |
|---|---|---|
| MySQL 8.4 | `localhost:${MYSQL_PORT:-3306}`, user `spark`/`spark`, root/`spark` | `mysql:3306` |
| Redpanda (Kafka API) | `localhost:9092` | `redpanda:29092` |
| Redis 7.4 | `localhost:6379` | `redis:6379` |
| Redpanda Console | http://localhost:8080 | — |

## Start / check

1. Run `.claude/skills/local-infra/scripts/infra-doctor.sh`. It checks Docker, flags
   host processes already holding the ports, and prints container health.
2. If it reports no conflicts: `docker compose up -d --wait`.
3. `--wait` can return while MySQL still says `health: starting` (it returns when
   `redpanda-init` exits). Poll before running jobs:
   `docker inspect -f '{{.State.Health.Status}}' mysql` until `healthy`.
4. `redpanda-init` showing `Exited (0)` is expected.
5. Confirm end to end with `sbt "runMain retail.setup.runSetup"` (prints the table list read through Spark).

## Stop / reset

- `docker compose down` stops everything and keeps the data volumes.
- `docker compose down -v` **deletes all MySQL, Redpanda and Redis data**. Only run it when
  the user asks for a reset, and say what will be lost. It is the only way to re-run
  `01-schema.sql`, which runs only when the volume is empty.

## Known problems

**`Access denied for user 'root'@'localhost'` although the container is healthy.**
Another MySQL on the host is answering `localhost:3306`. Docker-forwarded connections never
show up as `'root'@'localhost'`, so that host name in the error gives it away. This Mac has had
a native install at `/usr/local/mysql`. Check with `ps aux | grep [m]ysqld`.
Never stop the user's local server yourself. Offer two options:
- the user stops it (System Settings → MySQL), or
- `export MYSQL_PORT=3307`, then `docker compose up -d mysql` (recreates the container on the new port).

**Jobs or tests still hit the old port after changing `MYSQL_PORT`.** sbt 2 keeps a
background server, and forked test JVMs get that server's environment from when it started.
`runMain` runs on the client and does see the new value, which makes it confusing.
Fix: `sbt shutdown`, then re-run with the variable exported.

**Docker socket missing** (`.orbstack/run/docker.sock: no such file`): OrbStack isn't
running. Ask the user to start it rather than launching it yourself.

## Configuration knobs

`retail.config.InfraConfig.fromEnv()` reads `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_ROOT_PASSWORD`,
`MYSQL_USER`, `MYSQL_PASSWORD`, `KAFKA_BOOTSTRAP_SERVERS`, `REDIS_HOST`, `REDIS_PORT` and
`RAW_DATA_DIR`. Compose only reads `MYSQL_PORT`. When adding a service or port, update
compose, `InfraConfig`, this table, and `infra-doctor.sh` together.
