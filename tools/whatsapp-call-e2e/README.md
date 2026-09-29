# Real WhatsApp call E2E harness

This tool validates one real 1:1 WhatsApp audio call. In `inbound` mode it
waits for a real CallOffer, answers it, connects to `/call/stream/:callId`,
records inbound PCM16LE to a WAV file, sends a deterministic 440/880/660 Hz
marker, and hangs up. In `outbound` mode it calls one ordinary WhatsApp
number through `/call/dial`, waits for peer acceptance, runs the same bridge,
and hangs up.

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
The harness configures `subscribe: ["CALL"]`, selects the first incoming
CallOffer in inbound mode, and uses HMAC stream auth when `-signing-key` is set.
Without it, the legacy query apikey stream auth is used.

Outbound example:

    go run ./tools/whatsapp-call-e2e \
      -mode outbound -number 3519XXXXXXXX \
      -api http://127.0.0.1:4000 -instance "$EVOLUTION_INSTANCE" \
      -apikey "$EVOLUTION_INSTANCE_API_KEY" \
      -signing-key "$CALL_STREAM_SIGNING_KEY" -out-dir ./e2e-artifacts/outbound

Artifacts:

- inbound-<callId>.wav: received track, 16 kHz mono PCM16LE.
- lifecycle.jsonl: UTC lifecycle entries, call direction/state, per-frame
  timestamps, recorder counts, bridge counters, WAV path, and errors.

Audio evidence requires inboundFrames > 0 and nonZeroFrames > 0 in the WAV
recorder plus outboundFrames > 0 and outboundNonZeroFrames > 0 in the server
`server_diagnostics` event. The server's `bidirectionalMedia` flag is true
only for that two-sided non-zero bridge evidence. HTTP 200, `start`, ringing,
connected state, keepalive, or silence never pass validation.

The counters prove media crossed our bridge. They cannot prove that a human
heard the marker on the remote phone; the live run must also record that
audible confirmation in the test result.
