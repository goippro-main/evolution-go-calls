package main

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMarkerToneIsIdentifiableAndFrameAligned(t *testing.T) {
	samples, name, err := outbound("")
	if err != nil {
		t.Fatal(err)
	}
	if name != "generated-marker-440-880-660Hz" {
		t.Fatalf("unexpected marker name %q", name)
	}
	if len(samples)%frameSamples != 0 {
		t.Fatalf("marker has %d samples, not frame aligned", len(samples))
	}
	nonZero := 0
	for _, sample := range samples {
		if sample != 0 {
			nonZero++
		}
	}
	if nonZero == 0 {
		t.Fatal("marker is silent")
	}
}

func TestRecorderProducesPCM16WAVAndEvidenceSummary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbound.wav")
	rec, err := newRecorder(path)
	if err != nil {
		t.Fatal(err)
	}
	frame := make([]byte, frameBytes)
	for i := 0; i < frameSamples; i++ {
		binary.LittleEndian.PutUint16(frame[i*2:], uint16(int16(1000)))
	}
	if err := rec.add(frame); err != nil {
		t.Fatal(err)
	}
	summary := rec.summary()
	if summary["inboundFrames"] != int64(1) || summary["nonZeroFrames"] != int64(1) {
		t.Fatalf("unexpected summary: %#v", summary)
	}
	if summary["mediaEvidence"] != true {
		t.Fatalf("expected media evidence: %#v", summary)
	}
	rec.close()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != 44+frameBytes || string(raw[:4]) != "RIFF" || string(raw[8:12]) != "WAVE" {
		t.Fatalf("invalid WAV output: len=%d header=%q/%q", len(raw), string(raw[:4]), string(raw[8:12]))
	}
}

func TestHMACStreamURLDoesNotExposeSigningKey(t *testing.T) {
	stream, err := streamURL(cfg{
		api: "http://127.0.0.1:4000", instance: "test-instance",
		signing: "secret-signing-key",
	}, "call-123")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(stream, "secret-signing-key") || strings.Contains(stream, "apikey=") {
		t.Fatalf("secret or API key leaked into HMAC URL: %s", stream)
	}
	if !strings.HasPrefix(stream, "ws://127.0.0.1:4000/call/stream/call-123?") {
		t.Fatalf("unexpected stream URL: %s", stream)
	}
}

func TestWebhookOfferExtraction(t *testing.T) {
	payload := map[string]any{
		"event": "CallOffer",
		"data": map[string]any{
			"CallID":      "abc",
			"CallCreator": "351900000000@s.whatsapp.net",
		},
	}
	if got := first(payload, "CallID", "callId"); got != "abc" {
		t.Fatalf("unexpected call id %q", got)
	}
	if got := first(payload, "CallCreator", "callCreator"); got != "351900000000@s.whatsapp.net" {
		t.Fatalf("unexpected creator %q", got)
	}
}
