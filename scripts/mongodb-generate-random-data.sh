#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

usage() {
  cat <<USAGE
Usage: $(basename "$0") --container NAME [options]

Generate a realistic high-entropy MongoDB dataset until database size reaches
a target (default: ~10 GiB).

Required:
  --container NAME           Docker container name running MongoDB

Options:
  --target-gib N             Target database size in GiB (default: 10)
  --db NAME                  Database name (default: vykar_mongo_test)
  --batch-size N             Documents inserted per growth batch (default: 2000)
  --payload-bytes N          Approx bytes in random payload field (default: 8192)
  --max-batches N            Safety cap for growth loop (default: 20000)
  --progress-every N         Print progress every N batches (default: 5)
  --no-recreate-db           Keep existing DB and append data
  --help                     Show help

Examples:
  $(basename "$0") --container vykar-mongo --target-gib 10
  $(basename "$0") --container vykar-mongo --batch-size 1000 --payload-bytes 12288
USAGE
}

CONTAINER=""
TARGET_GIB=10
DB_NAME="vykar_mongo_test"
BATCH_SIZE=2000
PAYLOAD_BYTES=8192
MAX_BATCHES=20000
PROGRESS_EVERY=5
RECREATE_DB=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --container) CONTAINER="${2:-}"; shift 2 ;;
    --target-gib) TARGET_GIB="${2:-}"; shift 2 ;;
    --db) DB_NAME="${2:-}"; shift 2 ;;
    --batch-size) BATCH_SIZE="${2:-}"; shift 2 ;;
    --payload-bytes) PAYLOAD_BYTES="${2:-}"; shift 2 ;;
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
[[ "$TARGET_GIB" =~ ^[0-9]+$ ]] || die "--target-gib must be a non-negative integer"
[[ "$BATCH_SIZE" =~ ^[0-9]+$ && "$BATCH_SIZE" -gt 0 ]] || die "--batch-size must be > 0"
[[ "$PAYLOAD_BYTES" =~ ^[0-9]+$ && "$PAYLOAD_BYTES" -gt 0 ]] || die "--payload-bytes must be > 0"
[[ "$MAX_BATCHES" =~ ^[0-9]+$ && "$MAX_BATCHES" -gt 0 ]] || die "--max-batches must be > 0"
[[ "$PROGRESS_EVERY" =~ ^[0-9]+$ && "$PROGRESS_EVERY" -gt 0 ]] || die "--progress-every must be > 0"

need docker
need mktemp

TARGET_BYTES=$((TARGET_GIB * 1024 * 1024 * 1024))

mongo_eval() {
  local expr="$1"
  sudo -n docker exec "$CONTAINER" mongosh --quiet --eval "$expr"
}

log "waiting for MongoDB readiness in container=$CONTAINER"
ready=0
for _ in $(seq 1 180); do
  if [[ "$(mongo_eval "db.runCommand({ ping: 1 }).ok" 2>/dev/null | tr -d '[:space:]')" == "1" ]]; then
    ready=1
    break
  fi
  sleep 1
done
[[ "$ready" == "1" ]] || die "MongoDB not ready in container: $CONTAINER"

JS_FILE="$(mktemp /tmp/mongodb-random-data.XXXXXX.js)"
CONTAINER_JS_FILE="/tmp/$(basename "$JS_FILE")"
cleanup() {
  rm -f "$JS_FILE" >/dev/null 2>&1 || true
  sudo -n docker exec "$CONTAINER" sh -lc "rm -f '$CONTAINER_JS_FILE'" >/dev/null 2>&1 || true
}
trap cleanup EXIT

cat >"$JS_FILE" <<'JS'
const dbName = process.env.VYKAR_DB_NAME;
const targetBytes = Number(process.env.VYKAR_TARGET_BYTES);
const batchSize = Number(process.env.VYKAR_BATCH_SIZE);
const payloadBytes = Number(process.env.VYKAR_PAYLOAD_BYTES);
const maxBatches = Number(process.env.VYKAR_MAX_BATCHES);
const progressEvery = Number(process.env.VYKAR_PROGRESS_EVERY);
const recreateDb = process.env.VYKAR_RECREATE_DB === "1";

if (!dbName || !Number.isFinite(targetBytes) || targetBytes < 0) {
  throw new Error("invalid script inputs");
}

const targetPayloadHexLen = payloadBytes * 2;
const testDb = db.getSiblingDB(dbName);

function randInt(maxExclusive) {
  return Math.floor(Math.random() * maxExclusive);
}

function randChoice(values) {
  return values[randInt(values.length)];
}

function randomHex(len) {
  let out = "";
  while (out.length < len) {
    out += Math.random().toString(16).slice(2);
  }
  return out.slice(0, len);
}

function randomIp() {
  return `${randInt(256)}.${randInt(256)}.${randInt(256)}.${randInt(256)}`;
}

if (recreateDb) {
  testDb.dropDatabase();
}

testDb.customers.createIndex({ customer_id: 1 }, { unique: true });
testDb.products.createIndex({ product_id: 1 }, { unique: true });
testDb.orders.createIndex({ order_id: 1 }, { unique: true });
testDb.order_events.createIndex({ event_ts: 1 });
testDb.order_events.createIndex({ event_type: 1 });

if (recreateDb) {
  const customerTarget = 120000;
  const productTarget = 30000;
  const orderTarget = 250000;

  for (let start = 1; start <= customerTarget; start += 5000) {
    const docs = [];
    const end = Math.min(start + 4999, customerTarget);
    for (let i = start; i <= end; i++) {
      docs.push({
        customer_id: i,
        customer_uuid: UUID(),
        email: `user_${randomHex(12)}@example.test`,
        full_name: `${randomHex(8)} ${randomHex(10)}`,
        country_code: randChoice(["US", "CA", "GB", "DE", "FR", "BR", "IN", "JP", "AU", "SE"]),
        signup_ts: new Date(Date.now() - randInt(1825 * 24 * 3600 * 1000)),
        credit_score: 300 + randInt(550),
        lifetime_value_cents: NumberLong(randInt(50_000_000)),
      });
    }
    testDb.customers.insertMany(docs, { ordered: false });
  }

  for (let start = 1; start <= productTarget; start += 5000) {
    const docs = [];
    const end = Math.min(start + 4999, productTarget);
    for (let i = start; i <= end; i++) {
      docs.push({
        product_id: i,
        sku: `SKU-${randomHex(12)}`,
        title: `Product-${randomHex(16)}`,
        category: randChoice(["books", "electronics", "home", "games", "garden", "fitness", "automotive"]),
        unit_price_cents: 100 + randInt(200000),
        weight_grams: 50 + randInt(20000),
        attrs: {
          color: randChoice(["red", "blue", "green", "black", "white", "silver"]),
          rating: Number((Math.random() * 5).toFixed(2)),
          fragile: Math.random() > 0.85,
          batch: randomHex(10),
        },
      });
    }
    testDb.products.insertMany(docs, { ordered: false });
  }

  for (let start = 1; start <= orderTarget; start += 5000) {
    const docs = [];
    const end = Math.min(start + 4999, orderTarget);
    for (let i = start; i <= end; i++) {
      const unit = 100 + randInt(200000);
      const qty = 1 + randInt(8);
      docs.push({
        order_id: i,
        customer_id: 1 + randInt(customerTarget),
        product_id: 1 + randInt(productTarget),
        order_ts: new Date(Date.now() - randInt(730 * 24 * 3600 * 1000)),
        quantity: qty,
        unit_price_cents: unit,
        tax_cents: randInt(20000),
        discount_cents: randInt(10000),
        net_amount_cents: NumberLong(unit * qty),
        status: randChoice(["new", "paid", "shipped", "delivered", "returned", "cancelled"]),
        score: Number((Math.random() * 100).toFixed(4)),
        currency: randChoice(["USD", "EUR", "GBP", "JPY"]),
        note: randomHex(256),
        metadata: {
          channel: randChoice(["web", "mobile", "partner", "api"]),
          campaign: randomHex(10),
          priority: randInt(10),
          coupon: randomHex(6),
        },
      });
    }
    testDb.orders.insertMany(docs, { ordered: false });
  }
}

let batch = 0;
while (true) {
  const stats = testDb.stats(1);
  const bytes = Number(stats.dataSize || 0);
  if (bytes >= targetBytes) {
    break;
  }

  batch += 1;
  if (batch > maxBatches) {
    throw new Error(`reached max batches (${maxBatches}) before target size`);
  }

  const maxOrderDoc = testDb.orders.find().sort({ order_id: -1 }).limit(1).toArray()[0];
  const maxOrderId = maxOrderDoc ? maxOrderDoc.order_id : 1;

  const docs = [];
  for (let i = 0; i < batchSize; i++) {
    docs.push({
      order_id: 1 + randInt(maxOrderId),
      event_ts: new Date(Date.now() - randInt(365 * 24 * 3600 * 1000)),
      event_type: randChoice(["created", "authorized", "captured", "settled", "refunded", "chargeback"]),
      source: randChoice(["api", "mobile", "web", "batch", "worker"]),
      latency_ms: randInt(5000),
      risk_score: Number((Math.random() * 100).toFixed(4)),
      amount_cents: NumberLong(randInt(500000)),
      currency: randChoice(["USD", "EUR", "GBP", "JPY"]),
      note: randomHex(512),
      metadata: {
        session: randomHex(24),
        ip: randomIp(),
        device: randChoice(["ios", "android", "linux", "windows", "mac"]),
        build: 100000 + randInt(900000),
        flags: [randInt(10), randInt(10), randInt(10)],
      },
      payload: randomHex(targetPayloadHexLen),
    });
  }
  testDb.order_events.insertMany(docs, { ordered: false });

  if (batch % progressEvery === 0) {
    const p = testDb.stats(1);
    const e = testDb.order_events.estimatedDocumentCount();
    print(`[fill] batch=${batch} bytes=${p.dataSize} events=${e}`);
  }
}

const finalStats = testDb.stats(1);
print(`final_db_bytes=${finalStats.dataSize}`);
print(`final_db_pretty=${finalStats.dataSize / 1024 / 1024 / 1024}GiB`);
print(`customers=${testDb.customers.estimatedDocumentCount()}`);
print(`products=${testDb.products.estimatedDocumentCount()}`);
print(`orders=${testDb.orders.estimatedDocumentCount()}`);
print(`order_events=${testDb.order_events.estimatedDocumentCount()}`);
const sample = testDb.order_events.aggregate([{ $sample: { size: 1 } }, { $project: { _id: 0, payload: { $substrBytes: ["$payload", 0, 64] } } }]).toArray();
if (sample.length === 1) {
  print(`sample_event_payload_prefix=${sample[0].payload}`);
}
JS

log "seeding/filling db=$DB_NAME target_bytes=$TARGET_BYTES (~${TARGET_GIB}GiB)"
sudo -n docker cp "$JS_FILE" "$CONTAINER:$CONTAINER_JS_FILE"
sudo -n docker exec \
  -e VYKAR_DB_NAME="$DB_NAME" \
  -e VYKAR_TARGET_BYTES="$TARGET_BYTES" \
  -e VYKAR_BATCH_SIZE="$BATCH_SIZE" \
  -e VYKAR_PAYLOAD_BYTES="$PAYLOAD_BYTES" \
  -e VYKAR_MAX_BATCHES="$MAX_BATCHES" \
  -e VYKAR_PROGRESS_EVERY="$PROGRESS_EVERY" \
  -e VYKAR_RECREATE_DB="$RECREATE_DB" \
  "$CONTAINER" mongosh --quiet "$CONTAINER_JS_FILE"
