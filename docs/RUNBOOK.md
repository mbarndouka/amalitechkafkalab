# Heartbeat Monitoring Runbook

This runbook covers the local Docker deployment for the heartbeat Kafka pipeline.

## Start And Verify

```zsh
cp .env.example .env
docker compose build
docker compose up -d
docker compose ps
```

Expected healthy services:

- `postgres`: healthy
- `kafka`: healthy
- `consumer`: healthy
- `producer`: healthy
- `grafana`: running
- `kafka-ui`: running

## Main URLs

- Grafana: http://localhost:3000
- Kafka UI: http://localhost:8080
- PostgreSQL: `localhost:5432`
- Kafka host bootstrap: `localhost:9092`

Grafana loads the `Heartbeat Pipeline Overview` dashboard from source control.
Alert rules are visible under Grafana Alerting.

## Routine Checks

Check recent persisted records:

```zsh
docker compose exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT COUNT(*) FROM heart_rate_logs WHERE recorded_at > NOW() - INTERVAL '\''60 seconds'\'';"'
```

Check latest records:

```zsh
docker compose exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT event_id, customer_id, heart_rate, status, recorded_at FROM heart_rate_logs ORDER BY recorded_at DESC LIMIT 10;"'
```

Check topic creation:

```zsh
docker compose logs kafka-init
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list
```

## Logs

Producer, consumer, and healthcheck logs are JSON.

```zsh
docker compose logs -f --tail=100 consumer
docker compose logs -f --tail=100 producer
docker compose logs -f --tail=100 kafka
docker compose logs -f --tail=100 db-migrate
```

Important consumer log messages:

- `heartbeat_processed`: message was decoded and classified.
- `database_insert_failed`: Postgres write failed; Kafka offset is not committed.
- `message_scheduled_for_retry`: consumer rewound to retry a failed message.
- `kafka_offset_commit_failed`: database write succeeded but offset commit failed.

Important producer log messages:

- `heartbeat_batch_dispatched`: batch was queued for Kafka delivery.
- `kafka_delivery_failed`: Kafka delivery callback reported a failed send.
- `producer_queue_full`: producer local queue was full and backed off.

## Alert Rules

Provisioned Grafana alert rules:

- `No heartbeat records in last 2 minutes`: critical. Checks for stalled ingestion.
- `Critical heartbeat spike`: warning. Checks for more than 10 CRITICAL records in 5 minutes.

If an alert fires:

1. Check service health with `docker compose ps`.
2. Check `consumer` logs for insert or commit failures.
3. Check `producer` logs for Kafka delivery failures.
4. Check `kafka-init` logs to confirm the topic exists.
5. Query Postgres for recent records.

## Common Incidents

### No Records Persisted

Likely causes:

- Consumer is stopped or unhealthy.
- Kafka topic was not created.
- Postgres is unhealthy.
- Consumer cannot commit or insert.

Commands:

```zsh
docker compose ps
docker compose logs --tail=100 consumer
docker compose logs --tail=100 postgres
docker compose logs --tail=100 kafka-init
```

### Kafka Is Unhealthy On First Startup

Kafka can take longer than one healthcheck window on a cold start.

```zsh
docker compose ps
docker compose logs --tail=120 kafka
docker compose up -d
```

### Database Migrations Fail

Commands:

```zsh
docker compose logs db-migrate
docker compose up db-migrate
```

If a migration fails because existing data violates a new constraint, inspect the failing rows, fix or remove invalid data, then rerun `db-migrate`.

### Grafana Dashboard Missing

Commands:

```zsh
docker compose logs --tail=100 grafana
docker compose restart grafana
```

Confirm these directories are mounted:

- `grafana/provisioning/datasources`
- `grafana/provisioning/dashboards`
- `grafana/provisioning/alerting`
- `grafana/dashboards`

## Stop Services

```zsh
docker compose stop
```

Remove containers but keep Postgres data:

```zsh
docker compose down
```

Remove containers and delete Postgres data:

```zsh
docker compose down -v
```
