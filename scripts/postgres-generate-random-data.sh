#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

usage() {
  cat <<USAGE
Usage: $(basename "$0") --container NAME [options]

Generate a realistic high-entropy PostgreSQL dataset until database size
reaches a target (default: ~10 GiB).

Required:
  --container NAME           Docker container name running PostgreSQL

Options:
  --target-gib N             Target database size in GiB (default: 10)
  --db NAME                  Database name (default: vykar_pg_test)
  --postgres-db NAME         Admin DB for control commands (default: postgres)
  --user USER                PostgreSQL user (default: postgres)
  --password PASS            PostgreSQL password (default: testpass)
  --rows-per-batch N         Rows inserted per growth batch (default: 20000)
  --max-batches N            Safety cap for growth loop (default: 400)
  --progress-every N         Print progress every N batches (default: 1)
  --no-recreate-db           Keep existing database and append data
  --help                     Show help

Examples:
  $(basename "$0") --container vykar-pg --target-gib 10
  $(basename "$0") --container vykar-pg --rows-per-batch 10000
USAGE
}

CONTAINER=""
TARGET_GIB=10
DB_NAME="vykar_pg_test"
POSTGRES_DB="postgres"
PG_USER="postgres"
PG_PASSWORD="testpass"
ROWS_PER_BATCH=20000
MAX_BATCHES=400
PROGRESS_EVERY=1
RECREATE_DB=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --container) CONTAINER="${2:-}"; shift 2 ;;
    --target-gib) TARGET_GIB="${2:-}"; shift 2 ;;
    --db) DB_NAME="${2:-}"; shift 2 ;;
    --postgres-db) POSTGRES_DB="${2:-}"; shift 2 ;;
    --user) PG_USER="${2:-}"; shift 2 ;;
    --password) PG_PASSWORD="${2:-}"; shift 2 ;;
    --rows-per-batch) ROWS_PER_BATCH="${2:-}"; shift 2 ;;
    --max-batches) MAX_BATCHES="${2:-}"; shift 2 ;;
    --progress-every) PROGRESS_EVERY="${2:-}"; shift 2 ;;
    --no-recreate-db) RECREATE_DB=0; shift ;;
    --help|-h) usage; exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
done

[[ -n "$CONTAINER" ]] || die "--container is required"
[[ "$CONTAINER" =~ ^[a-zA-Z0-9_.-]+$ ]] || die "invalid --container"
[[ "$DB_NAME" =~ ^[a-zA-Z0-9_]+$ ]] || die "invalid --db"
[[ "$POSTGRES_DB" =~ ^[a-zA-Z0-9_]+$ ]] || die "invalid --postgres-db"
[[ "$PG_USER" =~ ^[a-zA-Z0-9_]+$ ]] || die "invalid --user"
[[ "$TARGET_GIB" =~ ^[0-9]+$ ]] || die "--target-gib must be a non-negative integer"
[[ "$ROWS_PER_BATCH" =~ ^[0-9]+$ && "$ROWS_PER_BATCH" -gt 0 ]] || die "--rows-per-batch must be > 0"
[[ "$MAX_BATCHES" =~ ^[0-9]+$ && "$MAX_BATCHES" -gt 0 ]] || die "--max-batches must be > 0"
[[ "$PROGRESS_EVERY" =~ ^[0-9]+$ && "$PROGRESS_EVERY" -gt 0 ]] || die "--progress-every must be > 0"

need docker

TARGET_BYTES=$((TARGET_GIB * 1024 * 1024 * 1024))

pg_exec() {
  sudo -n docker exec -e PGPASSWORD="$PG_PASSWORD" "$CONTAINER" \
    psql -v ON_ERROR_STOP=1 -U "$PG_USER" "$@"
}

pg_exec_stdin() {
  sudo -n docker exec -i -e PGPASSWORD="$PG_PASSWORD" "$CONTAINER" \
    psql -v ON_ERROR_STOP=1 -U "$PG_USER" "$@"
}

pg_scalar() {
  local db="$1" sql="$2"
  pg_exec -At -d "$db" -c "$sql" | tr -d '[:space:]'
}

pg_ready() {
  sudo -n docker exec -e PGPASSWORD="$PG_PASSWORD" "$CONTAINER" \
    pg_isready -U "$PG_USER" >/dev/null 2>&1
}

log "waiting for PostgreSQL readiness in container=$CONTAINER"
ready=0
for _ in $(seq 1 180); do
  if pg_ready; then
    ready=1
    break
  fi
  sleep 1
done
[[ "$ready" == "1" ]] || die "PostgreSQL not ready in container: $CONTAINER"

if [[ "$RECREATE_DB" == "1" ]]; then
  log "recreating database db=$DB_NAME"
  pg_exec -d "$POSTGRES_DB" -c "DROP DATABASE IF EXISTS $DB_NAME;"
  pg_exec -d "$POSTGRES_DB" -c "CREATE DATABASE $DB_NAME;"
fi

log "ensuring schema in db=$DB_NAME"
pg_exec_stdin -d "$DB_NAME" <<'SQL'
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS customers (
  id BIGSERIAL PRIMARY KEY,
  customer_uuid UUID NOT NULL DEFAULT gen_random_uuid(),
  email TEXT NOT NULL,
  full_name TEXT NOT NULL,
  country_code CHAR(2) NOT NULL,
  signup_ts TIMESTAMPTZ NOT NULL,
  credit_score INT NOT NULL,
  lifetime_value_cents BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS products (
  id BIGSERIAL PRIMARY KEY,
  sku TEXT NOT NULL,
  title TEXT NOT NULL,
  category TEXT NOT NULL,
  unit_price_cents INT NOT NULL,
  weight_grams INT NOT NULL,
  attrs JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
  id BIGSERIAL PRIMARY KEY,
  customer_id BIGINT NOT NULL,
  product_id BIGINT NOT NULL,
  order_ts TIMESTAMPTZ NOT NULL,
  quantity INT NOT NULL,
  unit_price_cents INT NOT NULL,
  tax_cents INT NOT NULL,
  discount_cents INT NOT NULL,
  net_amount_cents BIGINT NOT NULL,
  status TEXT NOT NULL,
  score DOUBLE PRECISION NOT NULL,
  currency CHAR(3) NOT NULL,
  note TEXT NOT NULL,
  metadata JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS order_events (
  id BIGSERIAL PRIMARY KEY,
  order_id BIGINT NOT NULL,
  event_ts TIMESTAMPTZ NOT NULL,
  event_type TEXT NOT NULL,
  source TEXT NOT NULL,
  latency_ms INT NOT NULL,
  risk_score DOUBLE PRECISION NOT NULL,
  amount_cents BIGINT NOT NULL,
  metadata JSONB NOT NULL,
  note TEXT NOT NULL,
  payload TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_order_events_ts ON order_events(event_ts);
CREATE INDEX IF NOT EXISTS idx_order_events_type ON order_events(event_type);

ALTER TABLE order_events ALTER COLUMN note SET STORAGE EXTERNAL;
ALTER TABLE order_events ALTER COLUMN payload SET STORAGE EXTERNAL;
SQL

if [[ "$RECREATE_DB" == "1" ]]; then
  log "seeding baseline rows in db=$DB_NAME"
  pg_exec_stdin -d "$DB_NAME" <<'SQL'
INSERT INTO customers (email, full_name, country_code, signup_ts, credit_score, lifetime_value_cents)
SELECT
  format('user_%s@example.test', substr(md5(gen_random_uuid()::text), 1, 16)),
  initcap(substr(md5(random()::text), 1, 8)) || ' ' || initcap(substr(md5(random()::text), 1, 10)),
  (ARRAY['US','CA','GB','DE','FR','BR','IN','JP','AU','SE'])[1 + floor(random() * 10)::int],
  now() - (random() * interval '1825 days'),
  300 + (random() * 550)::int,
  (random() * 50000000)::bigint
FROM generate_series(1, 120000);

INSERT INTO products (sku, title, category, unit_price_cents, weight_grams, attrs)
SELECT
  format('SKU-%s', substr(md5(gen_random_uuid()::text), 1, 12)),
  format('Product-%s', substr(md5(random()::text), 1, 16)),
  (ARRAY['books','electronics','home','games','garden','fitness','automotive'])[1 + floor(random() * 7)::int],
  100 + (random() * 200000)::int,
  50 + (random() * 20000)::int,
  jsonb_build_object(
    'color', (ARRAY['red','blue','green','black','white','silver'])[1 + floor(random() * 6)::int],
    'rating', round((random() * 5)::numeric, 2),
    'fragile', (random() > 0.85),
    'batch', substr(md5(random()::text), 1, 10)
  )
FROM generate_series(1, 30000);

INSERT INTO orders (
  customer_id, product_id, order_ts, quantity, unit_price_cents,
  tax_cents, discount_cents, net_amount_cents, status, score, currency, note, metadata
)
SELECT
  (1 + floor(random() * 120000))::bigint,
  (1 + floor(random() * 30000))::bigint,
  now() - (random() * interval '730 days'),
  1 + floor(random() * 8)::int,
  100 + (random() * 200000)::int,
  (random() * 20000)::int,
  (random() * 10000)::int,
  (random() * 1500000)::bigint,
  (ARRAY['new','paid','shipped','delivered','returned','cancelled'])[1 + floor(random() * 6)::int],
  round((random() * 100)::numeric, 4)::double precision,
  (ARRAY['USD','EUR','GBP','JPY'])[1 + floor(random() * 4)::int],
  repeat(md5(random()::text || clock_timestamp()::text), 8),
  jsonb_build_object(
    'channel', (ARRAY['web','mobile','partner','api'])[1 + floor(random() * 4)::int],
    'campaign', substr(md5(random()::text), 1, 10),
    'priority', (random() * 10)::int,
    'coupon', substr(md5(random()::text), 1, 6)
  )
FROM generate_series(1, 250000);
SQL
fi

size_bytes="$(pg_scalar "$DB_NAME" "SELECT pg_database_size('$DB_NAME');")"
size_pretty="$(pg_exec -At -d "$DB_NAME" -c "SELECT pg_size_pretty(pg_database_size('$DB_NAME'));" | tr -d '\n')"
log "initial size bytes=$size_bytes pretty=$size_pretty target_bytes=$TARGET_BYTES (~${TARGET_GIB}GiB)"

batch=0
while [[ "$size_bytes" =~ ^[0-9]+$ ]] && (( size_bytes < TARGET_BYTES )); do
  batch=$((batch + 1))
  (( batch <= MAX_BATCHES )) || die "reached max batches without target size: max_batches=$MAX_BATCHES"

  max_order_id="$(pg_scalar "$DB_NAME" "SELECT COALESCE(max(id),1) FROM orders;")"

  pg_exec_stdin -d "$DB_NAME" -v chunk_rows="$ROWS_PER_BATCH" -v max_order_id="$max_order_id" <<'SQL'
SET synchronous_commit = off;
INSERT INTO order_events (
  order_id, event_ts, event_type, source, latency_ms, risk_score,
  amount_cents, metadata, note, payload
)
SELECT
  (1 + floor(random() * :max_order_id::numeric))::bigint,
  now() - (random() * interval '365 days'),
  (ARRAY['created','authorized','captured','settled','refunded','chargeback'])[1 + floor(random() * 6)::int],
  (ARRAY['api','mobile','web','batch','worker'])[1 + floor(random() * 5)::int],
  (random() * 5000)::int,
  round((random() * 100)::numeric, 4)::double precision,
  (random() * 500000)::bigint,
  jsonb_build_object(
    'session', substr(md5(random()::text || clock_timestamp()::text), 1, 24),
    'ip', format('%s.%s.%s.%s', (random() * 255)::int, (random() * 255)::int, (random() * 255)::int, (random() * 255)::int),
    'device', (ARRAY['ios','android','linux','windows','mac'])[1 + floor(random() * 5)::int],
    'build', (100000 + floor(random() * 900000))::int,
    'flags', jsonb_build_array((random() * 10)::int, (random() * 10)::int, (random() * 10)::int)
  ),
  repeat(md5(random()::text || clock_timestamp()::text), 16),
  repeat(md5(random()::text || clock_timestamp()::text), 64)
  || repeat(md5(random()::text || clock_timestamp()::text), 64)
  || repeat(md5(random()::text || clock_timestamp()::text), 64)
  || repeat(md5(random()::text || clock_timestamp()::text), 64)
FROM generate_series(1, :chunk_rows);
SQL

  if (( batch % PROGRESS_EVERY == 0 )); then
    size_bytes="$(pg_scalar "$DB_NAME" "SELECT pg_database_size('$DB_NAME');")"
    size_pretty="$(pg_exec -At -d "$DB_NAME" -c "SELECT pg_size_pretty(pg_database_size('$DB_NAME'));" | tr -d '\n')"
    events_count="$(pg_scalar "$DB_NAME" "SELECT COUNT(*) FROM order_events;")"
    log "progress batch=$batch size_bytes=$size_bytes size_pretty=$size_pretty order_events=$events_count"
  fi
done

final_size_bytes="$(pg_scalar "$DB_NAME" "SELECT pg_database_size('$DB_NAME');")"
final_size_pretty="$(pg_exec -At -d "$DB_NAME" -c "SELECT pg_size_pretty(pg_database_size('$DB_NAME'));" | tr -d '\n')"
customers_count="$(pg_scalar "$DB_NAME" "SELECT COUNT(*) FROM customers;")"
products_count="$(pg_scalar "$DB_NAME" "SELECT COUNT(*) FROM products;")"
orders_count="$(pg_scalar "$DB_NAME" "SELECT COUNT(*) FROM orders;")"
events_count="$(pg_scalar "$DB_NAME" "SELECT COUNT(*) FROM order_events;")"

log "complete db=$DB_NAME bytes=$final_size_bytes pretty=$final_size_pretty"
printf 'container=%s\ndb=%s\ntarget_bytes=%s\nfinal_bytes=%s\nfinal_pretty=%s\ncustomers=%s\nproducts=%s\norders=%s\norder_events=%s\n' \
  "$CONTAINER" "$DB_NAME" "$TARGET_BYTES" "$final_size_bytes" "$final_size_pretty" \
  "$customers_count" "$products_count" "$orders_count" "$events_count"
