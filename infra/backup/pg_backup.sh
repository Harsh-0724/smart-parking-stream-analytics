#!/bin/bash
# Nightly TimescaleDB dump. Install with:  0 2 * * * /opt/smart-parking/infra/backup/pg_backup.sh
# Keeps the last 14 dumps in $BACKUP_DIR (default /var/backups/smart-parking).
set -euo pipefail

cd "$(dirname "$0")/../.."
BACKUP_DIR="${BACKUP_DIR:-/var/backups/smart-parking}"
KEEP="${KEEP:-14}"
mkdir -p "$BACKUP_DIR"

set -a; source .env; set +a
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
docker compose -f docker-compose.yml -f docker-compose.prod.yml exec -T timescaledb \
  pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" --format=custom \
  > "$BACKUP_DIR/parking-$stamp.dump"

# Fail loudly on an empty dump instead of rotating good ones away.
test -s "$BACKUP_DIR/parking-$stamp.dump"
ls -1t "$BACKUP_DIR"/parking-*.dump | tail -n +$((KEEP + 1)) | xargs -r rm --
echo "backup written: $BACKUP_DIR/parking-$stamp.dump"
