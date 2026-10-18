# Broker + Grafana dashboard

Runs the broker itself, plus Prometheus + Grafana, as three containers.
Fully self-contained - no need to `go run ./cmd/broker` first.

> This directory's `docker-compose.yml` is the **local-dev** stack: Grafana
> there gives anonymous visitors Admin, and the broker advertises
> `localhost`. For a public server use `deploy/prod/` instead; see
> [docs/deployment.md](../docs/deployment.md).

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

   Starting is slow the first time: Docker pulls the images, and Grafana runs
   its database migrations on first boot, which took about 8 minutes on a
   laptop with a SATA SSD. Grafana's database is not kept in a volume, so it
   does this again whenever the container is recreated (every `docker compose
   down` followed by `up`). `docker compose stop` and `docker compose start`
   keep the container, and its database, so they are quick.

The Docker broker's raw `/metrics` is published on http://localhost:9102
(port 9101 inside the container). It is deliberately not 9101: that port is
what a natively-run broker uses, and Prometheus's `host.docker.internal:9101`
target would otherwise reach the container a second time and count every
metric twice.

## Running the broker natively instead

If you'd rather run the broker with `go run ./cmd/broker` (the faster loop
for day-to-day dev) and only use Docker for Prometheus + Grafana, that still
works: `prometheus.yml` scrapes both the compose broker and
`host.docker.internal:9101`, and whichever one is actually running answers -
the other just shows as a down target in Prometheus's own target list
(http://localhost:9090/targets), which is harmless. This is why the compose
broker's metrics are on host port 9102 and not 9101.

## Stop it

```
docker compose down
```

Prometheus and Grafana still start fresh each time (no dashboards/alerts to
lose - they're provisioned from files, not clicked together). The broker's
own data - topics, records and committed consumer offsets - persists in a
named volume (`broker-data`) across `docker compose down`/`up`, matching how
the native `go run ./cmd/broker` workflow already behaves. The topic list is
the `topics.json` file inside that volume. To wipe the data and start over:
`docker compose down -v`.
