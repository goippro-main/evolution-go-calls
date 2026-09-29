#!/usr/bin/env bash
set -euo pipefail

: "${EVOLUTION_API_URL:?set EVOLUTION_API_URL}"
: "${EVOLUTION_INSTANCE:?set EVOLUTION_INSTANCE}"
: "${EVOLUTION_INSTANCE_API_KEY:?set EVOLUTION_INSTANCE_API_KEY}"
: "${CALL_STREAM_SIGNING_KEY:?set CALL_STREAM_SIGNING_KEY}"
: "${WHATSAPP_TEST_NUMBER:?set WHATSAPP_TEST_NUMBER to one ordinary WhatsApp number}"

exec go run ./tools/whatsapp-call-e2e \
  -mode outbound \
  -number "$WHATSAPP_TEST_NUMBER" \
  -api "$EVOLUTION_API_URL" \
  -instance "$EVOLUTION_INSTANCE" \
  -apikey "$EVOLUTION_INSTANCE_API_KEY" \
  -signing-key "$CALL_STREAM_SIGNING_KEY" \
  "${@}"
