# Offline local voice AI plan

Status: implemented as a sidecar CLI in `tools/local-voice-ai`; no live
Evolution Go, WhatsApp session, launchd job, QR, relink, or Linux host changes.

The CLI takes an existing recorded WAV and produces:

- transcript via local `whisper-cli`;
- short reply via local LLM (`ollama` default, `llama-cli` supported for GGUF);
- final TTS WAV as 16 kHz mono PCM16 padded to 960-sample frames.

Privacy posture:

- The input audio is read-only.
- Output directory is `0700`; text/audio artifacts are `0600`.
- `pipeline.jsonl` logs metadata and timings, not raw transcript/reply text,
  unless `-log-text` is explicitly supplied.
- No cloud APIs or paid services are used by the default path.

Reproducible offline command:

```bash
go run ./tools/local-voice-ai \
  -in /private/tmp/wa-call-e2e-harness-live/artifacts/inbound-0021FD9BF68A4917D9AB50E764DEC7A8.wav \
  -out-dir /private/tmp/local-voice-ai-e2e \
  -whisper-model "/Users/valera/Library/Application Support/voiceink/models/ggml-base.bin" \
  -llm-provider ollama \
  -ollama-model gemma3:4b \
  -voice auto
```

Validation:

```bash
go test ./tools/local-voice-ai
afinfo /private/tmp/local-voice-ai-e2e/response-16k-mono-pcm16-960frames.wav
jq . /private/tmp/local-voice-ai-e2e/run.json
```

Next live integration plan:

1. Keep the existing `tools/whatsapp-call-e2e` and launchd harness untouched.
2. Fix and prove inbound media capture reliability first: real call Active,
   non-zero inbound frames, valid inbound WAV with audible human speech.
3. Add an opt-in harness flag that calls `tools/local-voice-ai` for a recorded
   utterance and uses its `response-16k-mono-pcm16-960frames.wav` as the
   existing outbound WAV source.
4. Run one controlled live test only after explicit approval for that test
   window.
