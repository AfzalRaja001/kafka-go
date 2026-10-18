#!/usr/bin/env bash
# Update the broker on the VM to the latest code and restart it. This is also
# the "bring it back" command: if Oracle reclaims an idle instance, create a
# new one, run scripts/bootstrap-vm.sh, restore deploy/prod/.env, and run this.
#
# Run it from anywhere on the VM, as your normal user:
#   bash scripts/redeploy.sh
#
# Environment switches:
#   MONITORING=1   also start Prometheus and Grafana (loopback only)
#   NO_PULL=1      skip "git pull" and rebuild whatever is checked out
#
# Data lives in the broker-data volume and survives this command. It does not
# survive "docker compose down -v", and it does not survive the VM itself
# being deleted.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

ENV_FILE=deploy/prod/.env
COMPOSE_FILE=deploy/prod/docker-compose.prod.yml
PORT=9092

say() { printf '\n==> %s\n' "$*"; }

if [ ! -f "$ENV_FILE" ]; then
  echo "$ENV_FILE does not exist. Create it first:" >&2
  echo "  cp deploy/prod/.env.example $ENV_FILE   # then set KAFKA_ADVERTISED_HOST" >&2
  exit 1
fi
if ! grep -Eq '^KAFKA_ADVERTISED_HOST=[^[:space:]]+' "$ENV_FILE"; then
  echo "KAFKA_ADVERTISED_HOST is empty in $ENV_FILE. Set it to this VM's public IP." >&2
  exit 1
fi

compose=(docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE")
if [ "${MONITORING:-0}" = "1" ]; then
  compose+=(--profile monitoring)
fi

if [ "${NO_PULL:-0}" != "1" ]; then
  say "Updating the code"
  git pull --ff-only
fi

say "Building and starting"
"${compose[@]}" up -d --build --remove-orphans

say "Waiting for the broker to accept connections on port $PORT"
ready=0
for _ in $(seq 1 60); do
  if (echo > "/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then
    ready=1
    break
  fi
  sleep 1
done
if [ "$ready" -ne 1 ]; then
  echo "The broker did not start listening within 60 seconds. Recent logs:" >&2
  "${compose[@]}" logs --tail 40 broker >&2
  exit 1
fi

say "Status"
"${compose[@]}" ps
echo
"${compose[@]}" logs --tail 5 broker

# Old image layers pile up with every rebuild, on a 50 GB disk.
docker image prune -f >/dev/null

host=$(grep -E '^KAFKA_ADVERTISED_HOST=' "$ENV_FILE" | head -n1 | cut -d= -f2-)
say "Up. From another machine, check it with:"
echo "  kafka-topics.sh --bootstrap-server $host:$PORT --list"
