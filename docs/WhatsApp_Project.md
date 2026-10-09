## Update 2026-10-09 15:13 Europe/Lisbon - spoken TTS heard over WhatsApp
A real inbound WhatsApp call confirmed audible spoken outbound audio. Call ID `006AE5ED03D73C7F396E9A53CEFD5BB4` used harness `-outbound-mode wav` with source `/private/tmp/wa-call-e2e-harness-live/runtime-audio/spoken-marker-16k-20261009T140925Z.wav`; this is not the previous formant/tone marker. The user reported that the answer was heard, the sound was imperfect, and the words were intelligible. Server stats: outbound 162 frames / 162 nonzero / 150649 nonzero samples / peak 25719 / 0 dropped frames. Inbound remained absent for this call (`inboundFrames=0`, header-only inbound WAV), so STT and conversational AI are the next unresolved path. Live services were not restarted; backup is `/private/tmp/wa-call-e2e-confirmed-speech-backup-20261009T141710Z`.

# WhatsApp Project

Date: 2026-10-08
Repository: https://github.com/goippro-main/evolution-go-calls
Branch: codex/whatsapp-voice-calls
Commit: branch HEAD after the final push for this handoff. See the final operator note or `git rev-parse origin/codex/whatsapp-voice-calls`.

## Purpose

This branch is a call-focused Evolution Go fork for real 1:1 WhatsApp audio calls. It is meant to let an already paired WhatsApp test instance accept inbound calls, open a WebSocket media bridge, record inbound PCM to WAV, inject deterministic audio back into the call, and exercise an experimental outbound call path.

The project is not a finished contact-center product. It is a reproducible backend plus E2E harness for controlled WhatsApp audio-call research and debugging.

## Architecture

The server keeps the normal Evolution Go API and adds call-control and media streaming around `whatsmeow` plus the vendored `meowcaller` media/signaling stack.

Core path for inbound calls:

```text
WhatsApp peer
  -> whatsmeow raw call events
  -> meowcaller incoming call object
  -> instance-scoped CallRegistry
  -> webhook CALL event
  -> POST /call/answer
  -> GET /call/stream/:callId
  -> WebSocket media bridge
  -> harness WAV recorder + outbound marker injection
```

Core path for outbound calls:

```text
POST /call/dial
  -> meowcaller.Client.Call(number)
  -> CallRegistry.StoreOutgoing(instanceID, call)
  -> peer_accept / active signaling
  -> GET /call/stream/:callId
  -> WebSocket media bridge
```

The bridge uses mono 16 kHz PCM16LE in JSON WebSocket messages. Server diagnostics count inbound/outbound frames, nonzero samples, peaks, timestamps, dropped outbound frames, and a `bidirectionalMedia` boolean.

## Components

- `pkg/call/handler` exposes call-control HTTP endpoints.
- `pkg/call/service` binds API requests to instance-scoped active calls.
- `pkg/call/registry` stores inbound and outbound call objects by instance.
- `pkg/call/stream` implements HMAC-authenticated WebSocket media streaming, PCM codec conversion, playout gating, and diagnostics.
- `pkg/whatsmeow/service` initializes `meowcaller` before connect, stores incoming calls, forwards call events, and routes meowcaller logs into the instance logger.
- `third_party/meowcaller` is the local source snapshot used by `go.mod` via `replace github.com/purpshell/meowcaller => ./third_party/meowcaller`.
- `tools/whatsapp-call-e2e` is the real-call harness for inbound/outbound testing.

## Key Paths

```text
cmd/evolution-go/main.go
pkg/routes/routes.go
pkg/call/handler/call_handler.go
pkg/call/service/call_service.go
pkg/call/registry/call_registry.go
pkg/call/stream/handler.go
pkg/call/stream/bridge.go
pkg/call/stream/codec.go
pkg/whatsmeow/service/whatsmeow.go
pkg/whatsmeow/service/meowcaller_logging.go
tools/whatsapp-call-e2e/main.go
tools/whatsapp-call-e2e/README.md
docs/whatsapp-call-e2e.md
docs/handoffs/2026-10-08-whatsapp-live-calls-status.md
docs/WhatsApp_Project.md
third_party/meowcaller/
```

Excluded runtime/build artifacts must stay out of git: local executables (`evolution-go`, `whatsapp-call-e2e`), logs, WAV captures, DB files, sessions, QR/auth material, real API keys, instance tokens, cookies, pairing codes, and credentials.

## Setup And Run

Clone the working branch:

```bash
git clone --branch codex/whatsapp-voice-calls https://github.com/goippro-main/evolution-go-calls.git
cd evolution-go-calls
```

Use local-only secrets via environment variables or an ignored env file. Do not commit real values.

Minimum runtime shape:

```bash
export SERVER_PORT=4000
export GLOBAL_API_KEY='<local-admin-key>'
export POSTGRES_AUTH_DB='postgresql://<user>:<password>@<host>:<port>/<auth-db>?sslmode=disable'
export POSTGRES_USERS_DB='postgresql://<user>:<password>@<host>:<port>/<users-db>?sslmode=disable'
export CONNECT_ON_STARTUP=false
export DATABASE_SAVE_MESSAGES=false
export CALL_STREAM_SIGNING_KEY='<local-stream-signing-key>'
go run ./cmd/evolution-go
```

Run inbound harness after the instance is already paired and connected:

```bash
EVOLUTION_API_URL=http://127.0.0.1:4000 \
EVOLUTION_INSTANCE='<instance-id>' \
EVOLUTION_INSTANCE_API_KEY='<instance-token>' \
CALL_STREAM_SIGNING_KEY='<local-stream-signing-key>' \
go run ./tools/whatsapp-call-e2e \
  -mode inbound \
  -webhook-listen 127.0.0.1:8090 \
  -webhook-url http://127.0.0.1:8090/webhook \
  -out-dir /tmp/evolution-go-call-e2e \
  -max-calls 1
```

For a persistent listener, omit `-max-calls`. For outbound, use `-mode outbound -number '<ordinary-whatsapp-number>'` only after explicit operator approval for a real call.

## Current Call Flow

Inbound:

1. `meowcaller.NewClient` is created before `client.Connect()` so raw call stanzas are captured.
2. `OnIncomingCall` stores the call in the instance-scoped registry.
3. A `CallOffer` webhook reaches the harness.
4. Harness answers via `/call/answer`.
5. Harness opens `/call/stream/:callId`.
6. Server subscribes the bridge as both `Receive` sink and `Play` source.
7. Inbound streams emit `media_ready` before `CallPhaseActive` so the harness can start playout without waiting for inbound RTP.
8. Harness records inbound media, injects a 440/880/660 Hz marker, validates nonzero media, and hangs up.

Outbound:

1. Harness calls `/call/dial`.
2. The service stores the outgoing `meowcaller.Call`.
3. Stream opens for that call.
4. `peer_accept` or active state releases media.
5. Harness records inbound media, injects marker, validates evidence, and hangs up.

## Fixes Preserved In This Branch

- Inbound and outbound call objects are both registry-backed and instance-scoped.
- `/call/dial` exists as an experimental outbound call endpoint.
- Stream auth supports short-lived HMAC URLs bound to `instance|callId|exp`; legacy query `apikey` remains for non-browser compatibility.
- WebSocket stream disconnect no longer forgets or hangs up the active call by itself.
- Inbound streams allow outbound playout before `CallPhaseActive` and send `media_ready` to avoid callee-speaks-first deadlock.
- Outbound media is separately gated by `outboundReady`; inbound frame exposure is separately gated by `inboundReady`.
- Bridge diagnostics include inbound/outbound nonzero counts, peaks, timestamps, drops, and bidirectional evidence.
- Harness listener binds synchronously, stays alive across inbound attempts, supports `-max-calls`, logs WebSocket close/read errors, and continues after a failed inbound stream.
- Meowcaller logger is wired into the instance logger with `MEOWCALLER_LOG_LEVEL`.
- Vendored `meowcaller` includes call/signaling/media guards, including pre-accept `rejected_elsewhere` handling, self/non-peer busy reject handling, and RTP/SRTP receive instrumentation.
- Docs and tests cover the current reproducible state.

## Tests

Relevant test targets:

```bash
go test ./pkg/call/... ./pkg/whatsmeow/service ./tools/whatsapp-call-e2e
go test ./...
go build ./...
```

The branch intentionally treats HTTP 200, synthetic webhook offers, and open ports as insufficient proof. Success requires a real call reaching Active, nonzero inbound and outbound audio frames, valid WAV output, and audible marker evidence.

## Live Status

Current live status is not a pass. Earlier real tests reached CallOffer, `/call/answer` 200, and `/call/stream` 200, but the stream closed early or never reached usable inbound media. Some later calls dropped immediately or stayed ringing. Synthetic webhook tests and unit tests are useful but do not prove live WhatsApp media.

Do not claim READY/PASS until a controlled real call proves bidirectional media.

## Known Issue: Inbound Media

The main unresolved issue is live inbound media. The current evidence shows no reliable nonzero inbound RTP/audio reaching the WebSocket bridge in real calls. Possible areas to inspect:

- ICE/DTLS/WebRTC or WhatsApp relay negotiation.
- Call state transition order around preaccept/accept/transport/relay latency.
- Whether `CallPhaseActive` is blocked by missing receive-side media.
- Whether early stream close is a symptom or cause of WhatsApp-side teardown.
- Whether the vendored SRTP/RTP receive path is decoding the specific live traffic shape.

## Diagnostics

Use these evidence sources before touching live runtime:

- Harness `lifecycle.jsonl`: webhook events, answer/dial result, stream open, read/close errors, hangup, WAV summary.
- WebSocket `diagnostics` event: inbound/outbound frame counters and `bidirectionalMedia`.
- Evolution Go instance logs with `MEOWCALLER_LOG_LEVEL=debug` only for controlled diagnostics.
- Call webhook events: `CallOffer`, `CallAccept`, `CallTerminate`, `CallOfferNotice`, `CallRelayLatency`, `CallTransport`.
- Go tests for stream gating, auth, disconnect behavior, and harness persistence.

Never log raw keys, real tokens, QR material, cookies, sessions, or call media in git-tracked files.

## Exact Next Steps

1. Pull the branch and verify the final pushed commit SHA.
2. Re-run the relevant Go tests and build locally.
3. Review latest harness/runtime logs for the failed real inbound attempts.
4. Correlate one real `CallOffer -> Answer -> Stream -> Transport/Relay -> Active/Terminate` timeline.
5. Inspect `third_party/meowcaller` receive-side instrumentation around RTP/SRTP decrypt and media pipeline activation.
6. Add the smallest failing unit or synthetic integration test for the identified signaling/media edge.
7. Patch the receive/activation path.
8. Run unit tests, harness tests, and build.
9. Only after explicit operator approval, run one controlled inbound live call on the existing paired PT-egress setup.
10. Mark success only if real Active state, nonzero inbound/outbound frames, valid WAV, and audible marker are all present.

## Safety Constraints

- Do not initiate real calls without explicit approval.
- Do not log out, relink, re-pair, or rotate the existing WhatsApp account unless explicitly requested.
- Do not change PT egress, proxy, account, or device path during code-only work.
- Do not touch live launchd/system runtime except for explicitly approved test/build operations.
- Do not commit build binaries, logs, WAV captures, DBs, sessions, QR/auth artifacts, real API keys, tokens, credentials, or cookies.
- Do not merge this branch to `main` until live E2E media evidence passes.
