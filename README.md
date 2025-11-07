# Database Migrations

This directory contains SQL migration scripts for the Nexo Trading Bot database.

## How to Apply Migrations

### Using psql command line:
```bash
psql -h <host> -U <username> -d <database> -f migrations/001_add_notification_preferences.sql
```

### Using Python (psycopg):
```python
import psycopg
conn = psycopg.connect("your_connection_string")
with open('migrations/001_add_notification_preferences.sql', 'r') as f:
    conn.execute(f.read())
conn.commit()
```

### Using environment variable:
```bash
psql $DATABASE_URL -f migrations/001_add_notification_preferences.sql
```

## Migration History

| File | Date | Description |
|------|------|-------------|
| 001_add_notification_preferences.sql | 2025-11-07 | Add mute_trade_notifications and mute_daily_summary columns to users table |
