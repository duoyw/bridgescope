#!/bin/bash

# ===========================================
# PostgreSQL Full Backup Script
# Function: Backup all roles + all databases
# Output: roles_globals.sql + .dump file for each database
# Author: Your Helper
# Please modify configuration according to actual situation before use
# ===========================================

# ============= Configuration Section =============
export PGPASSWORD="${PGPASSWORD:-postgres}" # Recommended to set password via environment variable, or use ~/.pgpass
PGUSER="${PGUSER:-postgres}"                     # Default user
PGHOST="${PGHOST:-localhost}"                    # Host
PGPORT="${PGPORT:-5432}"                         # Port

BACKUP_DIR="/home/admin/yuchu.yc/mcp4db/backup_db" # Modify to your backup path
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
BACKUP_SUBDIR="$BACKUP_DIR/backup_$TIMESTAMP"

# Create backup directory
mkdir -p "$BACKUP_SUBDIR"

# Log file
LOG_FILE="$BACKUP_SUBDIR/backup.log"
exec > >(tee -i "$LOG_FILE")
exec 2>&1

echo "【Backup Started】Time: $(date)"
echo "Backup directory: $BACKUP_SUBDIR"
echo "Database host: $PGHOST:$PGPORT, user: $PGUSER"

# ============= 1. Backup global objects (roles, tablespaces) =============
echo
echo "【1/3】Backing up global objects (roles, tablespaces)..."
pg_dumpall \
  --host="$PGHOST" \
  --port="$PGPORT" \
  --username="$PGUSER" \
  --globals-only \
  --file="$BACKUP_SUBDIR/roles_and_tablespaces.sql"

if [ $? -eq 0 ]; then
  echo "✅ Global objects backup successful: $BACKUP_SUBDIR/roles_and_tablespaces.sql"
else
  echo "❌ Global objects backup failed"
  exit 1
fi

# ============= 2. Get database list (exclude template0, template1, postgres optional) =============
echo
echo "【2/3】Getting database list..."

DB_LIST=$(psql \
  --host="$PGHOST" \
  --port="$PGPORT" \
  --username="$PGUSER" \
  --tuples-only \
  --no-align \
  -c "SELECT datname FROM pg_database WHERE NOT datistemplate AND datname != 'postgres' ORDER BY datname;")

if [ -z "$DB_LIST" ]; then
  echo "⚠️ No user databases found"
fi

# ============= 3. Backup each database =============
echo
echo "【3/3】Starting to backup each database..."

for DB in $DB_LIST; do
  DB=$(echo "$DB" | tr -d '\r') # Clean carriage return (compatible with macOS/Linux)
  DUMP_FILE="$BACKUP_SUBDIR/${DB}_backup_${TIMESTAMP}.dump"
  echo "➡️ Backing up database: $DB -> $DUMP_FILE"

  pg_dump \
    --host="$PGHOST" \
    --port="$PGPORT" \
    --username="$PGUSER" \
    --format=custom \
    --blobs \
    --verbose \
    --file="$DUMP_FILE" \
    "$DB"

  if [ $? -eq 0 ]; then
    echo "✅ Database '$DB' backup successful"
  else
    echo "❌ Database '$DB' backup failed"
  fi
done

# ============= 4. Extra: Backup postgres database (optional) =============
echo
echo "➡️ Backing up system database: postgres"
pg_dump \
  --host="$PGHOST" \
  --port="$PGPORT" \
  --username="$PGUSER" \
  --format=custom \
  --blobs \
  --verbose \
  --file="$BACKUP_SUBDIR/postgres_backup_${TIMESTAMP}.dump" \
  postgres

if [ $? -eq 0 ]; then
  echo "✅ Database 'postgres' backup successful"
else
  echo "❌ Database 'postgres' backup failed"
fi

# ============= 5. Completion =============
echo
echo "【Backup Complete】All files saved to: $BACKUP_SUBDIR"
echo "📌 For next restore, please execute:"
echo "   psql -U $PGUSER -f roles_and_tablespaces.sql   # Restore roles"
echo "   Then use pg_restore to restore each .dump file"
