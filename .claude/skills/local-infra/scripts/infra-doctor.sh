#!/usr/bin/env bash
# Pre-flight and health check for the docker-compose stack. Read-only: it never
# starts, stops or kills anything.
set -u
# Project root = four levels above this script (.claude/skills/local-infra/scripts)
cd "$(cd "$(dirname "$0")" && pwd)/../../../.." || exit 2
[ -f docker-compose.yml ] || { echo "Run from the project root (docker-compose.yml not found)"; exit 2; }

MYSQL_PORT="${MYSQL_PORT:-3306}"
status=0

echo "== Docker"
if docker info --format 'server {{.ServerVersion}}' 2>/dev/null; then :; else
  echo "FAIL: Docker daemon not reachable (is OrbStack/Docker Desktop running?)"; exit 1
fi

echo "== Host ports (non-Docker listeners are conflicts)"
for port in "$MYSQL_PORT" 9092 6379 8080; do
  owners=$(lsof -nP -iTCP:"$port" -sTCP:LISTEN 2>/dev/null | awk 'NR>1 {print $1}' | sort -u | tr '\n' ' ')
  if [ -z "$owners" ]; then
    echo "  $port free"
  elif echo "$owners" | grep -qiE 'orbstack|docker|com\.docke|vpnkit'; then
    echo "  $port used by Docker ($owners)"
  else
    echo "  $port CONFLICT: held by $owners"; status=1
  fi
done
if pgrep -fl '/usr/local/mysql/bin/mysqld|/opt/homebrew/.*/mysqld' >/dev/null 2>&1; then
  echo "  WARNING: a native mysqld is running on the host; it may capture localhost:$MYSQL_PORT"
  status=1
fi

echo "== Containers"
docker compose ps -a --format '  {{.Name}}\t{{.Status}}' 2>/dev/null || echo "  (stack not created)"

exit $status
