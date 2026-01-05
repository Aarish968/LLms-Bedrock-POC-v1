#!/bin/bash

# Column Lineage API entrypoint script

set -e

# Function to log messages
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1"
}

log "Starting Column Lineage API..."

# Wait for database to be ready (if needed)
if [ -n "$SNOWFLAKE_ACCOUNT" ]; then
    log "Checking database connectivity..."
    python -c "
from api.dependencies.database import DatabaseManager
try:
    db = DatabaseManager()
    if db.test_connection():
        print('Database connection successful')
    else:
        print('Database connection failed')
        exit(1)
except Exception as e:
    print(f'Database connection error: {e}')
    exit(1)
"
fi

# Run database migrations (if any)
# python -m alembic upgrade head

log "Database ready. Starting application..."

# Execute the main command
exec "$@"