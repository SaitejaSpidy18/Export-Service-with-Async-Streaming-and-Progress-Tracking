# Export Service with Async Streaming and Progress Tracking

## Overview

This service exports up to 10M `users` rows from PostgreSQL to CSV using:
- Async IO (FastAPI, asyncpg)
- Streaming in chunks
- Background jobs for long-running exports
- Progress tracking and resumable downloads (HTTP Range, gzip)

## Requirements

- Docker
- Docker Compose

## Setup

Copy environment variables:

   ```bash
   cp .env.example .env
Adjust values if needed.

Build and start the stack:

docker-compose up --build

On first run, Postgres will seed 10,000,000 users; this can take several minutes.

Verify seeding:
docker exec -it export_db psql -U exporter -d exports_db -c "SELECT COUNT(*) FROM users;"
API
GET /health
Health check.

POST /exports/csv
Starts an export job (returns exportId and status: pending).

Query parameters:

country_code (optional)

subscription_tier (optional)

min_ltv (optional)

columns (optional, comma-separated e.g. id,email,country_code)

delimiter (optional, default ,)

quoteChar (optional, default ")

GET /exports/{exportId}/status
Returns job status and progress.

GET /exports/{exportId}/download
Streams the CSV file. Supports:

Range header for partial downloads

Accept-Encoding: gzip for gzip-compressed streaming

DELETE /exports/{exportId}
Cancels an in-progress job.

Examples (PowerShell)
# Start an export
$resp = Invoke-WebRequest -Method POST "http://localhost:8080/exports/csv?country_code=US&min_ltv=500"
$data = $resp.Content | ConvertFrom-Json
$exportId = $data.exportId

# Poll status
(Invoke-WebRequest "http://localhost:8080/exports/$exportId/status").Content | ConvertFrom-Json

# Download when completed
Invoke-WebRequest "http://localhost:8080/exports/$exportId/download" -OutFile "export_$exportId.csv"

Tests
TBD or describe how to run them if you add tests.


This gives the evaluator a straightforward “how to run” path and documents endpoints, which is recommended for good API design.[1][2]

***

 Verify `seeds/` is correct

We already set this up, but Step 4 expects:

- `seeds/init.sql` – table schemas and indexes  
- `seeds/seed_users.sql` – 10M rows via `generate_series`

You already validated this with `SELECT COUNT(*) = 10000000`, so Step 2 is satisfied.

***

Confirm Docker and env wiring

You must be able to run **everything** with one command:

```powershell
docker-compose up --build

Check:

docker-compose.yml has db and app with:

db using postgres:15

db healthcheck with pg_isready

app depends_on db (service_healthy)

app memory limit 150m under deploy.resources.limits.memory

env_file: - .env and export volume ./exports:/app/exports.

.env.example documents all required env vars (you already have).

.env (local, not committed) exists and matches the example (or your chosen values).

 (Optional but good) Add at least a smoke test
Not strictly required, but you can drop a minimal test in tests/ later (e.g., pytest) that:

calls /health

initiates an export and checks 202

confirms /status returns the right shape.

For now, the core requirement is just having a tests/ directory; tests themselves are optional.
