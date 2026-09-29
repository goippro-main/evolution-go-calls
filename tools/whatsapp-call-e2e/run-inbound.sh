#!/usr/bin/env bash
set -euo pipefail

: "${EVOLUTION_API_URL:?set EVOLUTION_API_URL}"
: "${EVOLUTION_INSTANCE:?set EVOLUTION_INSTANCE}"
: "${EVOLUTION_INSTANCE_API_KEY:?set EVOLUTION_INSTANCE_API_KEY}"
: "${CALL_STREAM_SIGNING_KEY:?set CALL_STREAM_SIGNING_KEY}"

exec go run ./tools/whatsapp-call-e2e \
  -mode inbound \
  -api "$EVOLUTION_API_URL" \
  -instance "$EVOLUTION_INSTANCE" \
  -apikey "$EVOLUTION_INSTANCE_API_KEY" \
  -signing-key "$CALL_STREAM_SIGNING_KEY" \
  "${@}"
