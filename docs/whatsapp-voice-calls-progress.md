# WhatsApp Evolution Go handoff - 2026-10-09

This document is a durable handoff for the GoIPpro WhatsApp voice-calls work. It records verified runtime evidence only; it intentionally contains no secrets, QR data, sessions, keys, runtime logs, databases, cookies, or phone numbers.

## Confirmed spoken TTS over WhatsApp - 2026-10-09

- Date/time: 2026-10-09 15:13:21-15:13:32 Europe/Lisbon (14:13:21-14:13:32 UTC).
- Call ID: `006AE5ED03D73C7F396E9A53CEFD5BB4`.
- Runtime: live Mac instance `wa-call-e2e`, harness PID 60512, Evolution Go PID 55285, no service restart performed.
- Human verification: user confirmed after the real inbound WhatsApp call: "Ответ услышал! Звук не идеален, но слова разборчивы."
- Outbound source: `mode=wav`, not the legacy formant/tone path.
- Source WAV: `/private/tmp/wa-call-e2e-harness-live/runtime-audio/spoken-marker-16k-20261009T140925Z.wav`, SHA-256 `629eba08895b6d7d5aca755d93f5f84be49c0a729a1d065d0f5d936e21289c85`, PCM16 mono 16 kHz, 90236 bytes.
- Sent artifact: `/private/tmp/wa-call-e2e-harness-live/artifacts/outbound-006AE5ED03D73C7F396E9A53CEFD5BB4.wav`, SHA-256 `49b5b21ef8d98fac85a410dfbc98483405c330dfb84e74c3ca994fe98a4aa4d5`, PCM16 mono 16 kHz, 90284 bytes.
- Outbound media stats: `outboundQueuedFrames=163`, `outboundFrames=162`, `outboundNonZeroFrames=162`, `outboundNonZeroSamples=150649`, `outboundPeak=25719`, `droppedOutboundFrames=0`, first outbound at `2026-10-09T14:13:22.154962Z`, last outbound at `2026-10-09T14:13:32.0548Z`.
- Inbound media stats: `inboundFrames=0`, `inboundNonZeroFrames=0`, `inboundNonZeroSamples=0`, `inboundPeak=0`; inbound WAV is header-only, 44 bytes, SHA-256 `ba584a378b11d9e9c98736fd8c256fe1453a84ee4139416d24b07acff424f0fb`.
- Failure state: recorder/server reported no inbound audio, so STT/AI turn-taking is not yet validated. This call proves audible robot-to-human spoken WAV over WhatsApp, not a full AI conversation.
- Quality clue: speech was intelligible to the human but imperfect. Non-disruptive next tuning should compare WAV loudness/peak, try a cleaner 16 kHz PCM source with less clipping/codec stress, and add STT capture only after inbound media is present.
- Backup: `/private/tmp/wa-call-e2e-confirmed-speech-backup-20261009T141710Z` contains live plist, binaries, artifacts, server log, and repo snapshot.

## Verified live call

- Date/time: 2026-10-09 12:55:30 Europe/Lisbon.
- Call ID: `0021FD9BF68A4917D9AB50E764DEC7A8`.
- Runtime: Mac instance `Sala 2`.
- Connection state: Mac instance was connected/logged in; call was accepted and reached active state.
- Inbound media: 108 decoded PCM frames, 49 nonzero frames.
- Inbound capture: WAV duration 6.48 seconds.
- Outbound media: 159 nonzero frames.
- Human verification: user heard the test tones on the phone.
- Termination: hangup succeeded.

Conclusion: two-way WhatsApp media is confirmed for the Mac setup. This is not yet an AI voice dialogue; the current outbound audio is test tone media, not generated speech, and the inbound audio is not yet wired through a full STT/AI/TTS loop.

## Contrast with previous call

Previous call `00BCE4B2293B9C36F96840CD66295ACF` proved signaling and outbound audio progress but had zero inbound frames. The 2026-10-09 call above is the first recorded successful inbound nonzero media confirmation in this handoff chain.

## Code and deployment state

- Repository: `goippro-main/evolution-go-calls`.
- Working branch: `codex/whatsapp-voice-calls`.
- Deployed commit on Linux `.198`: `34d65978107ae26f962e1defc799a6adacdec1b2` (`feat: preserve WhatsApp call media diagnostics`).
- `.198` services and PostgreSQL are running.
- `.198` WhatsApp is intentionally disconnected because the current egress is UA/Kharkiv, not Portugal.
- Keep Mac unchanged. Keep Linux unpaired until the account/egress decision is made.
- Linux account choice remains open: use a new WhatsApp account or migrate/reuse the prior one only after an explicit decision.

## Operational constraints

Do not initiate live calls, relink/logout/QR, pair Linux, change egress, or change live services without explicit approval. Do not commit or publish secrets, QR material, stored sessions, auth databases, cookies, credentials, API keys, tokens, or runtime artifacts.

## Next technical step

Replace test tones with speech generation, add STT/AI handling for inbound audio, then validate an actual bidirectional AI voice dialogue. Separately solve PT egress for `.198` before pairing WhatsApp on Linux.

## Staged spoken marker change

The next code change is staged on the same branch without changing the live Mac runtime. The E2E harness outbound source defaults to an offline deterministic speech marker: "this is a WhatsApp voice bridge test from GoIPpro". It is generated locally as 16 kHz mono PCM16 audio, written to `outbound-<callId>.wav` in the selected out-dir, and sent over `/call/stream/:callId` in 960-sample frames only after the existing media-ready gate.

Selectable modes:

- `-outbound-mode speech` (default): offline generated spoken marker, no cloud API credentials.
- `-outbound-mode tone`: legacy 440/880/660 Hz marker.
- `-outbound-mode wav -outbound-wav ./marker.wav`: externally prepared 16 kHz mono PCM16 WAV.

Safe activation for a user-initiated inbound test call remains: keep the already running Mac service untouched, start only the harness from a clean working tree/build, wait for the operator to place the WhatsApp call, then validate `inbound-<callId>.wav`, `outbound-<callId>.wav`, and `server_diagnostics` for nonzero inbound/outbound media. Do not initiate calls, QR/relink/logout, pair Linux, or restart live launchd without explicit approval and rollback prepared.

## Durable save map

- Dispatcher task: `GOIPPRO-WA-EVO-20261009`.
- Notion page: `WhatsApp проект` (`3f350f3b-8104-81b7-acc9-f8508b82a7b4`).
- ai-memory page: `notes/whatsapp-evolution-go-progress.md`.
- PostgreSQL record key: `whatsapp-evolution-go-2026-10-09-media-confirmed` in `evolution-go-calls-postgres` / `evogo_users.ops_progress_saves`.
