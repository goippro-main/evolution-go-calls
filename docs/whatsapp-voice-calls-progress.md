# WhatsApp Evolution Go handoff - 2026-10-09

This document is a durable handoff for the GoIPpro WhatsApp voice-calls work. It records verified runtime evidence only; it intentionally contains no secrets, QR data, sessions, keys, runtime logs, databases, cookies, or phone numbers.

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

## Durable save map

- Dispatcher task: `GOIPPRO-WA-EVO-20261009`.
- Notion page: `WhatsApp проект` (`3f350f3b-8104-81b7-acc9-f8508b82a7b4`).
- ai-memory page: `notes/whatsapp-evolution-go-progress.md`.
- PostgreSQL record key: `whatsapp-evolution-go-2026-10-09-media-confirmed` in `evolution-go-calls-postgres` / `evogo_users.ops_progress_saves`.
