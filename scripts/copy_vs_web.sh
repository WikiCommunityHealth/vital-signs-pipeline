#!/usr/bin/env bash
set -euo pipefail


SRC_HOST="postgres-backend"
DST_HOST="postgres-frontend"
DB_NAME="vital_signs_web"
TABLE_NAME="vital_signs_metrics"


PGUSER="${POSTGRES_USER}"
PGPASSWORD="${POSTGRES_PASSWORD}"


if PGPASSWORD="$PGPASSWORD" psql \
  -h "$DST_HOST" -U "$PGUSER" -d "$DB_NAME" -v ON_ERROR_STOP=1 \
  -c "TRUNCATE TABLE ${TABLE_NAME};"
then
  PGPASSWORD="$PGPASSWORD" pg_dump \
    -h "$SRC_HOST" -U "$PGUSER" -d "$DB_NAME" \
    --data-only --no-owner --no-privileges --table "$TABLE_NAME" \
  | PGPASSWORD="$PGPASSWORD" psql \
      -h "$DST_HOST" -U "$PGUSER" -d "$DB_NAME" -v ON_ERROR_STOP=1
else
  PGPASSWORD="$PGPASSWORD" pg_dump \
    -h "$SRC_HOST" -U "$PGUSER" -d "$DB_NAME" \
    --schema-only --no-owner --no-privileges --table "$TABLE_NAME" \
  | PGPASSWORD="$PGPASSWORD" psql \
      -h "$DST_HOST" -U "$PGUSER" -d "$DB_NAME" -v ON_ERROR_STOP=1

  PGPASSWORD="$PGPASSWORD" pg_dump \
    -h "$SRC_HOST" -U "$PGUSER" -d "$DB_NAME" \
    --data-only --no-owner --no-privileges --table "$TABLE_NAME" \
  | PGPASSWORD="$PGPASSWORD" psql \
      -h "$DST_HOST" -U "$PGUSER" -d "$DB_NAME" -v ON_ERROR_STOP=1
fi

echo "Copied succesfully"