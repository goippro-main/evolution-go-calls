# WhatsApp Evolution Go -> room_bridge.js adapter

Status: code and offline tests only. No live WhatsApp call, no production `.198`
WebSocket, no launchd/harness change, no QR/relink, no deployment.

## Current server protocol inspected

Read-only dispatcher job on `.198` inspected:

- Path: `/home/administrator/shared/aicc/spike/room_bridge.js`
- SHA256: `8f483fa740ae16d7e259d8a1a03a84747e24c383b52c307abe45880804c4b74b`
- Listener in source: `server.listen(PORT, '127.0.0.1')`, default `PORT=8191`
- Two-party WS query: `?dir=XX2YY&room=ROOM_ID`
- Inbound protocol: Vox-style nested JSON:

```json
{"event":"start","start":{"mediaFormat":{"encoding":"PCM16","sampleRate":16000,"channels":1}}}
{"event":"media","media":{"track":"inbound","payload":"<base64 PCM16>"}}
```

- Outbound protocol from room bridge: nested `event=media`, `media.track=outbound`,
  `media.payload=<base64 PCM16>`.

Evolution Go call stream remains top-level JSON:

```json
{"event":"media","track":"inbound","sampleRate":16000,"payload":"<base64 PCM16 960 samples>"}
{"event":"media","track":"outbound","sampleRate":16000,"payload":"<base64 PCM16 960 samples>"}
```

## Tool

New standalone tool:

```bash
go run ./tools/room-bridge-wa-adapter \
  -mode adapter \
  -evolution-ws-url 'ws://127.0.0.1:4000/call/stream/CALL_ID?instance=INSTANCE&exp=EXP&token=TOKEN' \
  -room-bridge-ws-url 'ws://127.0.0.1:8191' \
  -dir pt2ru
```

Controlled one-call inbound orchestrator:

```bash
go run ./tools/room-bridge-wa-adapter \
  -mode orchestrator \
  -api "$EVOLUTION_API_URL" \
  -instance "$EVOLUTION_INSTANCE" \
  -apikey "$EVOLUTION_INSTANCE_API_KEY" \
  -signing-key "$CALL_STREAM_SIGNING_KEY" \
  -webhook-listen 127.0.0.1:8090 \
  -webhook-url "$ORCHESTRATOR_WEBHOOK_URL" \
  -rollback-webhook-url "$PREVIOUS_HARNESS_WEBHOOK_URL" \
  -allow-caller '351900000001@s.whatsapp.net' \
  -configure-cutover \
  -call-time-limit 90s \
  -room-bridge-ws-url 'ws://127.0.0.1:8191' \
  -dir pt2ru
```

Security defaults:

- No default network endpoints. Both WS URLs are required.
- Evolution stream requires signed HMAC query params `instance`, `exp`, `token`.
  Legacy `apikey` is rejected unless `-allow-legacy-apikey` is explicitly set.
- `room_bridge.js` URL must be loopback. For `.198`, use a private tunnel such
  as local `127.0.0.1:8191 -> .198:127.0.0.1:8191`; do not connect directly to
  an internet-facing WS.
- Query secrets are redacted from adapter logs.
- The adapter creates a local callId-scoped lock file, preventing accidental
  duplicate local owners for the same Evolution call stream.
- The room ID is derived from the WhatsApp `callId`, e.g.
  `wa-call-007A27AF8C472B94720D093224287272`.
- Orchestrator mode requires an explicit `-allow-caller` allowlist, accepts only
  one inbound `CallOffer`, never calls `/call/dial`, signs the stream URL itself,
  hangs up on completion or `-call-time-limit`, and restores
  `-rollback-webhook-url` after success, failure, timeout, or ignored calls.
- Orchestrator mode does not log the instance token or stream signing key.

Runtime behavior:

- Evolution inbound `track=inbound` maps to room bridge nested
  `media.track=inbound`.
- Room bridge nested `media.track=outbound` maps back to Evolution top-level
  `track=outbound`.
- Evolution outbound frames are always split/padded to 960 samples at 16 kHz.
- Evolution `track=outbound` and room `track=inbound` are ignored to avoid a
  feedback loop.
- Room bridge is connected lazily only after the adapter sees speech above the
  RMS VAD threshold, reducing accidental Gemini Live session cost on silence.
  Set `-vad-threshold=0` only in a controlled lab run to disable this gate.
- Room bridge reconnect is bounded by `-max-room-reconnects`; Evolution stream
  reconnect is not automatic because Evolution allows only one stream owner.

## Offline validation

The tests use local `httptest` WebSocket doubles for both protocols. They verify:

- No network defaults.
- Signed Evolution URL requirement and default rejection of legacy `apikey`.
- Rejection of direct non-loopback room bridge URLs such as `.198`.
- Exact Evolution inbound PCM frame preservation into nested room media.
- Exact room outbound PCM preservation after splitting into 960-sample Evolution
  frames.
- callId-scoped room query generation.
- Silence-only input does not connect to room bridge.
- Lifecycle-only `start`/`state`/`media_ready` does not connect to room bridge;
  the Gemini room is opened only after inbound audio crosses VAD.
- Local lock blocks duplicate adapter owners for the same call.
- Orchestrator integration with mock Evolution HTTP+WS and fake loopback Gemini
  room_bridge, including allowlist rejection, answer failure, room failure,
  hangup, one-call behavior, HMAC stream URL, no `/call/dial`, and rollback.

Tests do not contact paid Gemini, production `.198`, Evolution live services,
WhatsApp, launchd, QR, or the protected backup.

## Controlled cutover requirement

The existing working Mac call stream/harness owns the Evolution WS during a call.
There must be exactly one stream owner. A live proof requires an explicit cutover
window:

1. Keep the protected backup untouched:
   `/Users/valera/whatsapp-evolution-go-stop-backups/20261010T145357+0100`.
2. Confirm the current harness webhook URL, e.g. `http://127.0.0.1:8090/webhook`,
   and pass it as `-rollback-webhook-url`.
3. Open a private loopback tunnel to `.198:127.0.0.1:8191`, or run an equivalent
   secured local room bridge endpoint. Do not expose an unauthenticated WS.
4. Start orchestrator mode with `-configure-cutover`, one exact `-allow-caller`,
   and a short `-call-time-limit`. It temporarily points `/instance/connect` at
   its webhook, answers only that inbound caller, creates the HMAC stream URL,
   owns the single stream, and bridges to loopback room_bridge.
5. Have the allowlisted user place exactly one WhatsApp audio call. Do not start
   the old harness as a competing stream owner during this window.
6. Wait for orchestrator exit. It sends `/call/hangup` and restores
   `-rollback-webhook-url` automatically. If it exits before answering, it still
   restores the previous webhook.
7. Only after logs show rollback restored should the previous harness be treated
   as active again.

Remaining live proof: one user-approved controlled WhatsApp call demonstrating
that room bridge Gemini audio returns to the WhatsApp call without a feedback
loop and without competing stream owners.
