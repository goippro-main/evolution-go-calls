# Local offline voice AI sidecar

`tools/local-voice-ai` is an offline-only CLI for the next WhatsApp voice AI
step. It does not touch Evolution Go, WhatsApp, QR pairing, launchd, webhooks,
or the live call harness. It only reads an existing recorded inbound audio file
and writes private local artifacts.

Pipeline:

1. Normalize input audio to 16 kHz mono PCM16 WAV with `ffmpeg`.
2. Transcribe with local `whisper-cli` and a local `ggml*.bin` model.
3. Generate a short reply with a local LLM (`ollama` by default, or
   `llama-cli` with a local GGUF path).
4. Synthesize TTS with macOS `say`, convert with `ffmpeg`, and pad the final
   WAV to exact 960-sample frames for WhatsApp media injection.

The JSONL log does not include transcript or reply text unless `-log-text` is
explicitly supplied. `transcript.txt`, `reply.txt`, `run.json`, and generated
audio are written under a `0700` output directory with `0600` files.

## Known local Mac inventory on 2026-10-09

- Hardware: Apple M3, 16 GB unified memory.
- Disk: `/private/tmp` is on a nearly full data volume, about 14 GiB free.
- STT: `/opt/homebrew/bin/whisper-cli`.
- Whisper models found:
  - `/Users/valera/Library/Application Support/voiceink/models/ggml-base.bin`
  - `/Users/valera/Library/Application Support/voiceink/models/ggml-tiny.bin`
  - `/Users/valera/Library/Application Support/com.pais.handy/models/whisper-medium-q4_1.bin`
  - `/Users/valera/Library/Application Support/com.pais.handy/models/ggml-large-v3-q5_0.bin`
- LLM: `/opt/homebrew/bin/ollama`, `/opt/homebrew/bin/llama-cli`.
- Installed Ollama models include `gemma3:4b`, `llama3.1`, `qwen2.5:7b`,
  `qwen2.5:14b`, `qwen2.5-coder:14b`, `llama3:8b`, and
  `deepseek-coder:1.3b`.
- Direct `.gguf` files were not found outside Ollama's blob store during the
  safe search. No model download is required for the default pipeline.
- TTS: macOS `/usr/bin/say`; Piper was not installed.

## Run

Use a non-empty existing inbound WAV artifact:

```bash
go run ./tools/local-voice-ai \
  -in /private/tmp/wa-call-e2e-harness-live/artifacts/inbound-0021FD9BF68A4917D9AB50E764DEC7A8.wav \
  -out-dir /private/tmp/local-voice-ai-e2e \
  -whisper-model "/Users/valera/Library/Application Support/voiceink/models/ggml-base.bin" \
  -llm-provider ollama \
  -ollama-model gemma3:4b \
  -voice auto
```

Useful alternatives:

```bash
# Better STT, slower, still local. Uses about 469 MB model on disk.
-whisper-model "/Users/valera/Library/Application Support/com.pais.handy/models/whisper-medium-q4_1.bin"

# llama.cpp path if a direct GGUF model is added later.
-llm-provider llama-cli -llama-model /path/to/model.gguf
```

Because free disk is low, safe future downloads should stay below roughly
3-4 GB unless space is freed first. Good candidates are small Q4 GGUF models
around 1-3B parameters, or continuing to use already installed Ollama models.
Do not download 14B-class models onto this volume without cleanup.

## Outputs

- `stt-input-16k-mono-pcm16.wav`: normalized input copy for STT.
- `transcript.txt`: STT text, private file.
- `reply.txt`: LLM reply that was spoken, private file.
- `tts.aiff`: raw `say` output.
- `tts-raw-16k-mono-pcm16.wav`: unpadded converted TTS WAV.
- `response-16k-mono-pcm16-960frames.wav`: final output, 16 kHz mono PCM16,
  sample count padded to a multiple of 960.
- `pipeline.jsonl`: metadata/timings only by default.
- `run.json`: reproducible paths, providers, timings, and audio stats.

`-voice auto` selects `Milena` for Cyrillic replies and `Joana` otherwise.
Pass an explicit macOS voice name when testing a fixed locale.

## Validation

```bash
go test ./tools/local-voice-ai

afinfo /private/tmp/local-voice-ai-e2e/response-16k-mono-pcm16-960frames.wav
jq . /private/tmp/local-voice-ai-e2e/run.json
```

The final WAV is ready for a later harness integration as an outbound source.
The next live step is to wire this sidecar behind the existing stream path only
after inbound media capture is reliable and a live test window is explicitly
approved.
