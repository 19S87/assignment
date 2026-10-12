# Zammad Round-Robin Auto Assignment

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)

A small webhook-driven service that assigns new Zammad tickets evenly across the eligible agents of a group. It plugs into Zammad Triggers + Webhooks and needs no changes to Zammad core.

Table of Contents
- [Overview](#overview)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Zammad Integration](#zammad-integration)
- [Behavior & Security](#behavior--security)
- [Data & Persistence](#data--persistence)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

---

## Overview

Zammad does not provide round-robin ticket assignment for groups out of the box. This service:

- Assigns tickets evenly across eligible agents of a group, in agent-id order
- Stores one cursor per group in a local SQLite database (no Redis needed)
- Skips users who are inactive, have no agent role, or are currently out of office
- Fails fast and loudly when Zammad rejects the API token, instead of retrying for minutes
- Periodically sweeps for new tickets that were missed (for example during a Zammad outage)

## Features

- Per-group round-robin with an atomic cursor (concurrent tickets never get the same agent)
- Non-blocking request handling: a slow or failing Zammad cannot freeze the service
- Retries with backoff for network errors, 429 and 5xx only; 401/403 are never retried
- Failover: if Zammad refuses one agent, the next one is tried
- HMAC (SHA1) webhook verification
- Structured logs and a `/health` endpoint that checks the Zammad token
- Works with Zammad Community and Enterprise

## Prerequisites

- Zammad (Docker-based setup recommended)
- Docker & Docker Compose
- A Zammad API token (Personal Access Token) for a user that may read users/groups and update `ticket.owner`

## Quick Start

### Clone

```bash
git clone https://github.com/19S87/zammad-round-robin-assignment.git
cd zammad-round-robin-assignment/assignment
```

### Configure environment variables

Create a `.env` file:

```
ZAMMAD_URL=https://your-zammad-domain
ZAMMAD_API_TOKEN=your_api_token_here
HMAC_SECRET=your_hmac_secret_here
```

Notes:
- `HMAC_SECRET` must match the secret configured in the Zammad webhook.
- Instead of `ZAMMAD_API_TOKEN` you can set `ZAMMAD_API_TOKEN_FILE=/run/secrets/zammad_token`. The file is re-read on every request, so rotating the token does not require a restart.

### Add to Docker Compose

```yaml
version: '3.8'
services:
  assignment:
    build:
      context: ./assignment
      dockerfile: Dockerfile
    container_name: zammad-rr-assignment
    env_file:
      - .env
    ports:
      - "8001:8001"
    volumes:
      - ./assignment/data:/data   # REQUIRED: holds the round-robin cursors
    restart: unless-stopped
```

### Start the service

```bash
docker compose -f docker-compose.yml -f scenarios/add-assignment.yml up -d
```

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `ZAMMAD_URL` | (required) | Base URL of Zammad |
| `ZAMMAD_API_TOKEN` | (required unless file is used) | API token |
| `ZAMMAD_API_TOKEN_FILE` | unset | Read the token from this file on every call |
| `HMAC_SECRET` | (required) | Webhook signing secret |
| `RR_DB_PATH` | `/data/round_robin.db` | SQLite file for the cursors |
| `ZAMMAD_CONNECT_TIMEOUT` / `ZAMMAD_READ_TIMEOUT` | `5` / `15` | Seconds per Zammad request |
| `ZAMMAD_MAX_ATTEMPTS` | `3` | Attempts for transient errors |
| `ZAMMAD_BACKOFF_SECONDS` | `1` | First backoff, doubles each attempt |
| `AGENT_CACHE_TTL_SECONDS` | `60` | How long a group's agent list is cached |
| `AGENT_ROLE_NAMES` | `Agent` | Comma-separated role names that count as agents |
| `OWNER_GROUP_ACCESS` | unset (off) | Optional: skip members whose *direct* group access has none of these levels (e.g. `full,change`). Leave unset if agents get access via roles or parent groups |
| `SWEEP_INTERVAL_SECONDS` | `300` | Sweeper period; `0` disables it |
| `SWEEP_MIN_AGE_SECONDS` | `60` | Sweeper ignores tickets younger than this |
| `SWEEP_QUERY` | `state.name:new AND owner_id:1` | Zammad search used by the sweeper |
| `LOG_LEVEL` | `INFO` | Python log level |

## Zammad Integration

### Create Webhook

Admin → Manage → Webhook (or Settings → Webhooks, depending on version). Create:

- Name: Round Robin Assignment
- Endpoint: `https://(your-domain)/assignment`
- Method: POST
- Secret: your `HMAC_SECRET`
- Custom payload: Yes

```json
{
  "ticket_id": "#{ticket.id}",
  "group_id": "#{ticket.group_id}"
}
```

Zammad sends the ids as strings; the service converts them.

### Create Trigger

Conditions
- State is: new
- Owner is: not set
- Action is: created

Execute
- Webhook → Round Robin Assignment

## Behavior & Security

- Eligible agents are active members of the group that hold an agent role (`AGENT_ROLE_NAMES`) and are not currently out of office (dates compared in the server's timezone).
- Before assigning, the service checks that the ticket still has no owner, so repeated webhook deliveries are harmless.
- Zammad's response to the owner update is used to confirm the assignment; no second request is made.
- Error handling: 401/403 from Zammad abort immediately with HTTP 502; network errors/429/5xx are retried with backoff and end as HTTP 503; a refusal for one agent (4xx such as 422) moves on to the next agent.
- Webhook requests are validated with HMAC-SHA1 (`X-Hub-Signature`).
- The service runs a single instance with a local SQLite file. Do not run several replicas against different volumes, or each will keep its own cursor.

## Data & Persistence

The only state is one row per group (`last_user_id`) in SQLite at `RR_DB_PATH` (default `/data/round_robin.db`). **Mount `/data` as a volume**; without it the cursors reset whenever the container is recreated and the first agent in each group receives a disproportionate share.

## Monitoring

- `GET /livez` – process is up (never calls Zammad).
- `GET /health` – calls Zammad `GET /api/v1/users/me` (cached 30 s). Returns 200 when the token works, 503 otherwise, and includes `last_assignment_ok_at`, `last_error`, `last_error_at`, `last_sweep_at`. Point your uptime monitor at this endpoint so a rejected or expired token alerts you before tickets pile up.
- Logs: `docker logs -f zammad-rr-assignment`. Each assignment logs ticket, group, agent, source (`webhook` or `sweeper`), attempts and duration; failures log the HTTP status and reason.

## Troubleshooting

- **Tickets stop being assigned / `/health` returns 503 with `ZammadAuthError`**: the token is rejected. Check Zammad → Profile → Token Access for an expiry date, that the token's user is still active, and that it still has the needed permissions. Create a new token, update `ZAMMAD_API_TOKEN` (or the token file) and recreate the container.
- **`/health` returns 503 with `ZammadTransientError`**: Zammad is unreachable or erroring; check `ZAMMAD_URL`, DNS and Zammad's own health.
- **HMAC validation failures (401)**: the webhook secret in Zammad must equal `HMAC_SECRET`.
- **"No assignable agents found for group"**: the log line `group=N direct members=... eligible=[...] excluded={id: reason}` shows who was considered and why each was dropped. Note that Zammad's group member list only contains users with *direct* access; agents who reach the group through a role or a parent group are not in it.
- **Uneven distribution after a restart**: make sure `/data` is a persistent volume.
- **Sweeper assigns nothing**: the default `SWEEP_QUERY` is validated against Zammad's search syntax at runtime; check the logs for search errors and adjust `SWEEP_QUERY` for your Zammad version if needed.

## Contributing

Contributions, issues, and feature requests are welcome. Please open an issue or submit a pull request.

## License

This project is licensed under the GNU Affero General Public License v3.0 (AGPL-3.0). See the `LICENSE` file for details or https://www.gnu.org/licenses/agpl-3.0 for the full text.
