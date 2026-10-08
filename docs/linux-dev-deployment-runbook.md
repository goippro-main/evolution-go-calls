# Linux Development Deployment Runbook

This runbook records the 2026-10-08 migration of the Evolution Go WhatsApp
calling development stack from the Mac rollback install to the GoIPpro Linux
development target.

## Target and Source

- Project target: `vps198`, `81.17.140.198`, project key `goippro`.
- Source repo: `goippro-main/evolution-go-calls`.
- Source branch: `codex/whatsapp-voice-calls`.
- Deployed source commit: `34d65978107ae26f962e1defc799a6adacdec1b2`.
- Linux checkout: `/home/administrator/shared/evolution-go-calls`.
- Runtime root: `/home/administrator/shared/evolution-go-calls-live`.
- Mac rollback install remains unchanged at `/private/tmp/evolution-go-calls`
  with its launchd services.

## Services

User-level systemd units are used, so this deployment does not require changing
root-owned GoIPpro production services:

- `evolution-go-calls-db.service`
  - Dockerized `postgres:16-alpine`.
  - Container: `evolution-go-calls-postgres`.
  - Local listener: `127.0.0.1:15432`.
  - Databases: `evogo_users`, `evogo_auth`.
- `evolution-go-calls.service`
  - Binary: `/home/administrator/shared/evolution-go-calls-live/bin/evolution-go`.
  - Working directory: `/home/administrator/shared/evolution-go-calls`.
  - API listener: `*:4000`.
  - Health endpoint: `http://127.0.0.1:4000/server/ok`.
- `wa-call-e2e-harness.service`
  - Binary: `/home/administrator/shared/evolution-go-calls-live/bin/whatsapp-call-e2e`.
  - Runs inbound listener only, with `-configure=false`.
  - Webhook listener: `127.0.0.1:18090`.
  - Does not initiate calls.

Secrets live only in `/home/administrator/shared/evolution-go-calls-live/*.env`
with `0600` permissions. Do not pass API keys or signing keys in systemd
`ExecStart` arguments.

## Network Egress Gate

WhatsApp account traffic must preserve the Mac test egress requirement:
`PT / MEO / Maia`. The Linux target default egress is currently
`81.17.140.198`, `UA / Kharkiv`, so the deployment is intentionally
fail-closed for WhatsApp network attempts:

- `CONNECT_ON_STARTUP=false`.
- Global proxy env is set to a closed local SOCKS endpoint:
  `PROXY_PROTOCOL=socks5`, `PROXY_HOST=127.0.0.1`, `PROXY_PORT=1`.
- The API and harness may run for build, health, DB and webhook verification.
- Do not run `/instance/connect`, `/instance/qr`, `/instance/pair`,
  `/call/dial`, live inbound call tests, or any relinking until
  `health/check-egress.sh` passes with PT/MEO/Maia.

## State

The Mac service uses PostgreSQL persistence, not standalone session files.
The supported persisted state was copied over SSH from the Mac PostgreSQL
databases into the Linux dev databases:

- `evogo_users`: instance metadata and runtime config.
- `evogo_auth`: whatsmeow auth/session tables.

This does not change WhatsApp account linkage by itself. On Linux, the restored
instance exists, but connection/login verification is blocked until the egress
gate is satisfied.

## Build and Test

Use an executable temporary directory because `/tmp` on `vps198` may reject Go
test binaries:

```bash
cd /home/administrator/shared/evolution-go-calls
export TMPDIR=/home/administrator/shared/evolution-go-calls-live/tmp
export GOTOOLCHAIN=auto
go test ./pkg/call/... ./tools/whatsapp-call-e2e
go test ./...
(cd third_party/meowcaller && go test ./...)
go build -o /home/administrator/shared/evolution-go-calls-live/bin/evolution-go ./cmd/evolution-go
go build -o /home/administrator/shared/evolution-go-calls-live/bin/whatsapp-call-e2e ./tools/whatsapp-call-e2e
```

## Health Checks

```bash
systemctl --user is-active evolution-go-calls-db.service evolution-go-calls.service wa-call-e2e-harness.service
ss -ltnp | grep -E ':(15432|4000|18090)\b'
/home/administrator/shared/evolution-go-calls-live/health/check-api.sh
/home/administrator/shared/evolution-go-calls-live/health/check-harness.sh
/home/administrator/shared/evolution-go-calls-live/health/check-egress.sh
```

Expected current result:

- API health passes via `/server/ok`.
- Harness health passes on `127.0.0.1:18090`.
- Egress health fails until a PT/MEO/Maia Linux egress path is provided.
- Instance status may show `Connected=false` and `LoggedIn=false` because
  Linux is not allowed to connect to WhatsApp on UA egress.

## Operations

Start:

```bash
systemctl --user start evolution-go-calls-db.service
systemctl --user start evolution-go-calls.service
systemctl --user start wa-call-e2e-harness.service
```

Stop:

```bash
systemctl --user stop wa-call-e2e-harness.service
systemctl --user stop evolution-go-calls.service
systemctl --user stop evolution-go-calls-db.service
```

Logs:

```bash
journalctl --user -u evolution-go-calls.service -u wa-call-e2e-harness.service -n 100 --no-pager
tail -f /home/administrator/shared/evolution-go-calls-live/logs/evolution-go.err.log
tail -f /home/administrator/shared/evolution-go-calls-live/artifacts/lifecycle.jsonl
```

## Rollback

Mac rollback remains the source of the last known logged-in local development
install. To roll back, stop the Linux services above and use the existing Mac
launchd services:

- `gui/501/com.valera.evolution-go-calls`
- `gui/501/com.valera.wa-call-e2e-harness`

Do not delete the Linux copied state until a controlled PT-egress Linux login
and live media test have been approved and completed.
