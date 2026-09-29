# Real WhatsApp call E2E harness

This tool waits for a real CallOffer from a paired Evolution instance,
answers it, connects to /call/stream/:callId, records inbound PCM16LE to a
WAV file, sends a deterministic 440/880/660 Hz marker, and hangs up.

It never creates or pairs an instance. Pairing remains an explicit operator
action with a dedicated test account.

Run from the repository root:

    go run ./tools/whatsapp-call-e2e \
      -api http://127.0.0.1:4000 \
      -instance "$EVOLUTION_INSTANCE" \
      -apikey "$EVOLUTION_INSTANCE_API_KEY" \
      -signing-key "$CALL_STREAM_SIGNING_KEY" \
      -webhook-url http://127.0.0.1:8090/webhook \
      -out-dir ./e2e-artifacts

If Evolution runs in Docker, the webhook URL must be reachable from the
container, for example http://host.docker.internal:8090/webhook, and the
listen address must accept that connection. API and signing keys are never
written to lifecycle logs.

The service must already be running and the instance must already be paired.
The harness configures subscribe: ["CALL"], selects the first incoming
CallOffer, answers it, and uses HMAC stream auth when -signing-key is set.
Without it, the legacy query apikey stream auth is used.

Artifacts:

- inbound-<callId>.wav: received track, 16 kHz mono PCM16LE.
- lifecycle.jsonl: UTC lifecycle entries, per-frame timestamps, media counts,
  non-zero/silent counts, WAV path, and errors.

Audio evidence requires ws_start, inbound media frames, and an audio_summary
with inboundFrames > 0, nonZeroFrames > 0, mediaEvidence: true. A start/stop
pair with zero media is not a successful audio validation. The bridge's
inbound frames come from meowcaller's audio callback; its outbound silence
fallback is not reported as inbound media.
