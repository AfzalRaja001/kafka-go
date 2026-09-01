# Grafana dashboard

Runs Prometheus + Grafana against a broker you start yourself - the broker
isn't a container here (see `docker-compose.yml`'s own comment for why).

## Run it

1. Start the broker, from the repo root:

   ```
   go run ./cmd/broker
   ```

2. From this directory (`deploy/`):

   ```
   docker compose up
   ```

3. Open http://localhost:3000 - the "kafka-go broker" dashboard is already
   there under Dashboards, no login, no manual datasource setup, no JSON
   import. Prometheus itself is at http://localhost:9090 if you want to run
   raw PromQL queries.

4. Send the broker some traffic (produce/consume with `kafka-python`, or
   `kcat`) and the panels should start filling in within a few seconds -
   Prometheus scrapes every 15s (`prometheus/prometheus.yml`), matching the
   broker's own metrics collection interval.

## Stop it

```
docker compose down
```

Nothing here persists data between runs - restarting `docker compose up`
starts Prometheus and Grafana fresh. That's fine for a dev dashboard; a real
deployment would add volumes for both, but that's out of scope for what
this exists for right now.
