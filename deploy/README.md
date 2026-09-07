# Broker + Grafana dashboard

Runs the broker itself, plus Prometheus + Grafana, as three containers.
Fully self-contained - no need to `go run ./cmd/broker` first.

## Run it

From this directory (`deploy/`):

```
docker compose up
```

(Use `docker compose up -d` to run it detached - if you run it in the
foreground and then reuse that same terminal for other commands, it
interrupts the stack. See docs/decisions.md for how this bit us once.)

The first run builds the broker image from `Dockerfile` (repo root as build
context), which takes a minute or so; later runs reuse the cached layers.

1. Open http://localhost:3000 - the "kafka-go broker" dashboard is already
   there under Dashboards, no login, no manual datasource setup, no JSON
   import. Prometheus itself is at http://localhost:9090 if you want to run
   raw PromQL queries.

2. Send the broker some traffic (produce/consume with `kafka-python`, or
   `kcat`, against `localhost:9092`) and the panels should start filling in
   within a few seconds - Prometheus scrapes every 15s
   (`prometheus/prometheus.yml`), matching the broker's own metrics
   collection interval.

## Running the broker natively instead

If you'd rather run the broker with `go run ./cmd/broker` (the faster loop
for day-to-day dev) and only use Docker for Prometheus + Grafana, that still
works: `prometheus.yml` scrapes both the compose broker and
`host.docker.internal`, and whichever one is actually running answers - the
other just shows as a down target in Prometheus's own target list
(http://localhost:9090/targets), which is harmless.

## Stop it

```
docker compose down
```

Prometheus and Grafana still start fresh each time (no dashboards/alerts to
lose - they're provisioned from files, not clicked together). The broker's
own data - topics, records, committed consumer offsets - persists in a named
volume (`broker-data`) across `docker compose down`/`up`, matching how the
native `go run ./cmd/broker` workflow already behaves. To wipe it and start
over: `docker compose down -v`.
