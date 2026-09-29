package main

import (
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestValidateMediaRejectsSilence(t *testing.T) {
	err := validateMedia("inbound", map[string]any{"inboundFrames": int64(3), "nonZeroFrames": int64(0)}, map[string]any{
		"outboundFrames": int64(3), "outboundNonZeroFrames": int64(3),
	})
	if err == nil {
		t.Fatal("expected silent inbound audio to fail")
	}
}

func TestValidateMediaRequiresServerOutboundEvidence(t *testing.T) {
	err := validateMedia("outbound", map[string]any{"inboundFrames": int64(3), "nonZeroFrames": int64(2)}, map[string]any{
		"outboundFrames": int64(0), "outboundNonZeroFrames": int64(0),
	})
	if err == nil {
		t.Fatal("expected missing outbound bridge media to fail")
	}
}

func TestValidateMediaPassesOnlyWithBothTracks(t *testing.T) {
	err := validateMedia("outbound", map[string]any{"inboundFrames": float64(3), "nonZeroFrames": float64(2)}, map[string]any{
		"outboundFrames": float64(3), "outboundNonZeroFrames": float64(3),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestWebhookFindsNestedCallOffer(t *testing.T) {
	var payload map[string]any
	if err := json.Unmarshal([]byte(`{"data":{"event":"CallOffer","callId":"abc","callCreator":"123@s.whatsapp.net"}}`), &payload); err != nil {
		t.Fatal(err)
	}
	if got := first(payload, "CallID", "callId"); got != "abc" {
		t.Fatalf("call id = %q", got)
	}
	if got := first(payload, "CallCreator", "callCreator"); got != "123@s.whatsapp.net" {
		t.Fatalf("call creator = %q", got)
	}
}

func TestPostJSONDecodesDialResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("apikey") != "secret" {
			t.Fatalf("missing apikey")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"callId":"CALL-1"}`))
	}))
	defer server.Close()
	var response struct {
		CallID string `json:"callId"`
	}
	if err := postJSON(server.Client(), server.URL, "secret", map[string]string{"number": "351900000000"}, &response); err != nil {
		t.Fatal(err)
	}
	if response.CallID != "CALL-1" {
		t.Fatalf("call id = %q", response.CallID)
	}
}

func TestRecorderWritesValidWAVHeader(t *testing.T) {
	path := t.TempDir() + "/audio.wav"
	recorder, err := newRecorder(path)
	if err != nil {
		t.Fatal(err)
	}
	frame := make([]byte, frameBytes)
	frame[0] = 1
	if err := recorder.add(frame); err != nil {
		t.Fatal(err)
	}
	recorder.close()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != int64(44+frameBytes) {
		t.Fatalf("WAV size = %d", info.Size())
	}
}

func TestPositiveCounterSupportsJSONNumbers(t *testing.T) {
	if got := positiveCounter(map[string]any{"n": float64(4)}, "n"); got != 4 {
		t.Fatalf("counter = %d", got)
	}
	if got := positiveCounter(map[string]any{"n": time.Duration(4)}, "n"); got != 0 {
		t.Fatalf("unexpected duration conversion = %d", got)
	}
}

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
