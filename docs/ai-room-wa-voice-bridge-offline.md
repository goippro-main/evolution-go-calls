# WhatsApp Evolution Go -> AI-Room voice bridge offline prototype

Status: code-only offline prototype. It does not start a live bridge, answer a
WhatsApp call, call WhatsApp, reconfigure Evolution Go, touch the protected Mac
backup, connect to `.198`, or connect to production AI-Room.

## Confirmed interfaces

Evolution Go call stream is `GET /call/stream/:callId`. The stream emits JSON
messages. Audio media uses 16 kHz mono PCM16LE in 960-sample frames:

```json
{"event":"media","track":"inbound","sampleRate":16000,"payload":"<base64 PCM16LE 960 samples>"}
```

Outbound audio is the same envelope with `"track":"outbound"`. This matches the
current source in `pkg/call/stream` and `tools/whatsapp-call-e2e`.

AI-Room web chat, from local repo inspection at
`/Users/valera/aicc-push/ai-room`, exposes:

- `GET /chat/<site_key>?lang=<lang>`: creates Flask session, chat, and
  participant.
- `POST /api/chat/send`: accepts `{"message":"..."}` and returns
  `{"ok":true,"reply":"..."}` using the Flask session.
- `/api/voice/rt-session` and `/api/voice/transcript`: browser/OpenAI Realtime
  path, not a direct bot participant API for a WhatsApp media bridge.

Therefore the prototype uses an injectable text-agent adapter. Tests use
`-agent=mock`. The `ai-room-http` adapter exists only for later local lab use and
requires explicit `-allow-network`; `.198` is hard-blocked.

## Tool

CLI:

```bash
go run ./tools/ai-room-voice-bridge \
  -in-wav /path/to/inbound-16k-mono-pcm16.wav \
  -out-dir /tmp/wa-ai-room-bridge-offline \
  -stt fixed \
  -fixed-transcript "Olá, quero testar a ponte offline." \
  -agent mock \
  -mock-reply "Recebido. Esta é uma resposta offline." \
  -tts tone
```

Whisper + macOS `say` offline path, still no AI-Room network:

```bash
go run ./tools/ai-room-voice-bridge \
  -in-wav /path/to/inbound-16k-mono-pcm16.wav \
  -out-dir /tmp/wa-ai-room-bridge-whisper \
  -stt whisper \
  -whisper-model "/Users/valera/Library/Application Support/voiceink/models/ggml-base.bin" \
  -language pt \
  -agent mock \
  -tts say \
  -voice auto
```

JSONL stream artifact path:

```bash
go run ./tools/ai-room-voice-bridge \
  -in-jsonl /path/to/captured-stream.jsonl \
  -out-dir /tmp/wa-ai-room-bridge-jsonl \
  -stt fixed \
  -agent mock \
  -tts tone
```

The output directory is `0700`; generated files are `0600`. Text is excluded
from `bridge.jsonl` unless `-log-text` is explicitly set.

Artifacts:

- `inbound-normalized-16k-mono-pcm16.wav`
- `transcript.txt`
- `reply.txt`
- `response-16k-mono-pcm16-960frames.wav`
- `outbound-media.jsonl`
- `bridge.jsonl`
- `run.json`

## Tests run

```bash
go test ./tools/ai-room-voice-bridge
```

Result:

```text
ok github.com/evolution-foundation/evolution-go/tools/ai-room-voice-bridge
```

## Safety controls

- No WebSocket dialer to Evolution Go live stream is implemented in this tool.
- No `/call/answer`, `/call/hangup`, `/instance/connect`, QR, relink, DB, or
  launchd operations.
- AI-Room HTTP adapter is opt-in only: `-agent=ai-room-http -allow-network`.
- Loopback AI-Room URLs are allowed for future local lab use.
- Non-loopback AI-Room URLs require `-allow-ai-host=<host>`.
- `81.17.140.198` is always blocked by code.
- Empty/silent input, empty STT, empty agent reply, and unaligned TTS output fail
  closed.

## Blockers before live deployment

1. Explicit approval for a live test window.
2. Decide whether AI-Room should expose a real bot participant API instead of
   relying on the Flask web-chat session.
3. Pick the canonical AI-Room runtime for WhatsApp voice; do not confuse the
   Mac working WhatsApp session with the separate `.198` setup.
4. Verify local Whisper model availability and OpenAI credit state if any
   non-local STT path is considered.
5. Add a separate live harness integration that can consume
   `outbound-media.jsonl` or the response WAV, with explicit operator approval
   before any call.
