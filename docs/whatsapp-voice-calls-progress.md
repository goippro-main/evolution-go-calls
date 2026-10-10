# WhatsApp Evolution Go handoff - 2026-10-10

This document is a durable handoff for the GoIPpro WhatsApp voice-calls work. It records verified runtime evidence only; it intentionally contains no secrets, QR data, sessions, keys, runtime logs, databases, cookies, or phone numbers.

## Verified stop point - 2026-10-10

- Date/time: 2026-10-10 14:47 WEST.
- Call ID: `007A27AF8C472B94720D093224287272`.
- Runtime: live Mac instance `Sala 2`, branch `codex/whatsapp-voice-calls`, commit `44e037bee64d74e4bfca50e619d51097fd449f7e` (includes `f829b7f`).
- Live services: `com.valera.evolution-go-calls` and `com.valera.wa-call-e2e-harness`.
- Live paths: `/Users/valera/evolution-go-calls-live` and `/Users/valera/wa-call-e2e-harness-live`.
- Human verification: English outbound TTS was heard during the real user-initiated inbound WhatsApp call.
- Inbound media: `92` frames, `43` nonzero frames, `inboundSpanMs=7445`, `inboundStalledMs=14`, `inboundPeak=31900`.
- Outbound media: `131` frames, `127` nonzero frames.
- Result markers: `media_validation_pass` and `call_attempt_succeeded`.
- Local private backup: `/Users/valera/whatsapp-evolution-go-stop-backups/20261010T145357+0100`, mode `drwx------ valera:staff`.
- Backup manifest: `SHA256SUMS.txt`, manifest SHA-256 `3e70491904aab2ea2db8bea7f3bc9f0d71bc21f056c84bb4c2a621e62e7b37ca`, verified with `shasum -a 256 -c`.
- Backup contents: live binaries, start scripts/config, LaunchAgent plists, logs, call WAV artifacts, current TTS marker WAV, git status, git bundle, redacted summary, restore procedure, and private restricted PostgreSQL custom dumps.
- Private DB snapshots: `private-db/evogo_auth_whatsapp_credentials.dump` and `private-db/evogo_users_progress_runtime.dump`, both mode `600`, kept only in the local private backup. Dump contents are not published in GitHub, Notion, dispatcher, or memory.
- Linux `.198`: intentionally unlinked for WhatsApp and untouched.

Operational stop rule: do not restart services, place calls, relink/QR, edit live config, or touch Linux `.198` WhatsApp unless explicitly approved. Do not upload env/start-script contents, sessions, logs, databases, WAVs, QR material, credentials, API keys, tokens, or cookies.

Next stage: build the voice AI path offline first: Whisper -> Ollama `gemma3:4b` -> macOS `say`; it is not live yet.

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

## Mac reboot recovery - 2026-10-09

After the Mac reboot at 2026-10-09 16:26 WEST, the `/private/tmp` checkout and runtime vanished. Safe recovery moved the Mac runtime to durable home paths without QR/relink:

- repo: `~/Projects/evolution-go-calls`
- Evolution runtime: `~/evolution-go-calls-live`
- harness runtime/artifacts: `~/wa-call-e2e-harness-live`
- launchd labels: `com.valera.evolution-go-calls` and `com.valera.wa-call-e2e-harness`
- preserved DB: Homebrew PostgreSQL 16 at `/opt/homebrew/var/postgresql@16`, databases `evogo_users` and `evogo_auth`

The harness now ignores replayed already-ended `CallOffer` webhooks (`is_call_ended=1` or `terminate_reason`) instead of trying `/call/answer` and creating stale 404 noise during recovery.

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
