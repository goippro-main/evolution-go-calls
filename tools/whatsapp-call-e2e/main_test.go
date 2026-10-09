package main

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestValidateMediaRejectsSilence(t *testing.T) {
	err := validateMedia("inbound", map[string]any{"inboundFrames": int64(3), "nonZeroFrames": int64(0), "nonZeroSamples": int64(0), "peak": int64(0)}, map[string]any{
		"outboundFrames": int64(3), "outboundNonZeroFrames": int64(3), "outboundNonZeroSamples": int64(1200), "outboundPeak": int64(1200),
	})
	if err == nil {
		t.Fatal("expected silent inbound audio to fail")
	}
}

func TestValidateMediaRejectsTinyNoise(t *testing.T) {
	err := validateMedia("inbound", map[string]any{"inboundFrames": int64(66), "nonZeroFrames": int64(1), "nonZeroSamples": int64(231), "peak": int64(118)}, map[string]any{
		"outboundFrames": int64(163), "outboundNonZeroFrames": int64(129), "outboundNonZeroSamples": int64(93627), "outboundPeak": int64(8749),
	})
	if err == nil {
		t.Fatal("expected tiny inbound noise to fail")
	}
}

func TestValidateMediaRequiresServerOutboundEvidence(t *testing.T) {
	err := validateMedia("outbound", map[string]any{"inboundFrames": int64(3), "nonZeroFrames": int64(3), "nonZeroSamples": int64(1200), "peak": int64(1200)}, map[string]any{
		"outboundFrames": int64(0), "outboundNonZeroFrames": int64(0), "outboundNonZeroSamples": int64(0), "outboundPeak": int64(0),
	})
	if err == nil {
		t.Fatal("expected missing outbound bridge media to fail")
	}
}

func TestValidateMediaPassesOnlyWithBothTracks(t *testing.T) {
	err := validateMedia("outbound", map[string]any{"inboundFrames": float64(82), "nonZeroFrames": float64(17), "nonZeroSamples": float64(13026), "peak": float64(31959)}, map[string]any{
		"outboundFrames": float64(156), "outboundNonZeroFrames": float64(124), "outboundNonZeroSamples": float64(90667), "outboundPeak": float64(8749),
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

func TestInboundRunContinuesAfterFailedStream(t *testing.T) {
	var answers atomic.Int64
	var hangups atomic.Int64
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/call/answer":
			answers.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"message":"success"}`))
		case "/call/hangup":
			hangups.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"message":"success"}`))
		case "/call/stream/call-1", "/call/stream/call-2":
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				t.Errorf("upgrade: %v", err)
				return
			}
			defer conn.Close()
			var start wsMessage
			if err := conn.ReadJSON(&start); err != nil {
				t.Errorf("read start: %v", err)
				return
			}
			_ = conn.WriteJSON(wsMessage{Event: "start", CallID: strings.TrimPrefix(r.URL.Path, "/call/stream/"), SampleRate: rate})
			_ = conn.WriteJSON(wsMessage{Event: "state", Phase: "connecting", Direction: "incoming"})
			_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "no media"), time.Now().Add(time.Second))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	logFile, err := os.Create(filepath.Join(t.TempDir(), "lifecycle.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()
	l := &logger{enc: json.NewEncoder(logFile)}
	offers := make(chan offer, 2)
	offers <- offer{ID: "call-1", Creator: "351900000001@s.whatsapp.net"}
	offers <- offer{ID: "call-2", Creator: "351900000002@s.whatsapp.net"}

	err = runCalls(t.Context(), server.Client(), cfg{
		api: server.URL, key: "secret", mode: "inbound", out: t.TempDir(),
		duration: 50 * time.Millisecond, timeout: time.Second, maxCalls: 2,
	}, offers, l)
	if err != nil {
		t.Fatal(err)
	}
	if answers.Load() != 2 {
		t.Fatalf("answers = %d, want 2", answers.Load())
	}
	if hangups.Load() != 2 {
		t.Fatalf("hangups = %d, want 2", hangups.Load())
	}
}

func TestStreamDurationDoesNotPanicOnQuietWebSocket(t *testing.T) {
	var hangups atomic.Int64
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/call/hangup":
			hangups.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"message":"success"}`))
		case "/call/stream/call-quiet":
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				t.Errorf("upgrade: %v", err)
				return
			}
			defer conn.Close()
			var start wsMessage
			if err := conn.ReadJSON(&start); err != nil {
				t.Errorf("read start: %v", err)
				return
			}
			_ = conn.WriteJSON(wsMessage{Event: "start", CallID: "call-quiet", SampleRate: rate})
			_ = conn.WriteJSON(wsMessage{Event: "state", Phase: "connecting", Direction: "incoming"})
			time.Sleep(3 * time.Second)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	logFile, err := os.Create(filepath.Join(t.TempDir(), "lifecycle.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()
	l := &logger{enc: json.NewEncoder(logFile)}

	_, err = stream(server.Client(), t.Context(), cfg{
		api: server.URL, key: "secret", mode: "inbound", out: t.TempDir(),
		duration: 10 * time.Millisecond, timeout: time.Second,
	}, "call-quiet", l)
	if err == nil || !strings.Contains(err.Error(), "media failed") {
		t.Fatalf("expected validation error without websocket panic, got %v", err)
	}
	if hangups.Load() != 1 {
		t.Fatalf("hangups = %d, want 1", hangups.Load())
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
	samples, name, err := outbound("tone", "")
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

func TestGeneratedSpeechMarkerIsFrameAlignedAndLevelSafe(t *testing.T) {
	samples, name, err := outbound("speech", "")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(name, "generated-speech-marker") {
		t.Fatalf("unexpected speech marker name %q", name)
	}
	if len(samples)%frameSamples != 0 {
		t.Fatalf("speech marker has %d samples, not frame aligned", len(samples))
	}
	if len(samples) < rate || len(samples) > 5*rate {
		t.Fatalf("speech marker duration out of expected range: samples=%d", len(samples))
	}
	var nonZero, peak int
	for _, sample := range samples {
		abs := int(sample)
		if abs < 0 {
			abs = -abs
		}
		if abs > 8 {
			nonZero++
		}
		if abs > peak {
			peak = abs
		}
	}
	if nonZero < rate/2 {
		t.Fatalf("speech marker has too few non-zero samples: %d", nonZero)
	}
	if peak == 0 || peak > 26000 {
		t.Fatalf("speech marker peak = %d, want nonzero and safely below clipping", peak)
	}
}

func TestOutboundWAVIsReadAndPaddedToFrameBoundary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "short.wav")
	samples := make([]int16, frameSamples+17)
	for i := range samples {
		samples[i] = int16(100 + i%20)
	}
	if err := writeWAV(path, samples); err != nil {
		t.Fatal(err)
	}
	got, name, err := outbound("wav", path)
	if err != nil {
		t.Fatal(err)
	}
	if name != path {
		t.Fatalf("wav source name = %q", name)
	}
	if len(got)%frameSamples != 0 {
		t.Fatalf("wav source not padded to frames: %d", len(got))
	}
	if len(got) != frameSamples*2 {
		t.Fatalf("wav source samples = %d, want %d", len(got), frameSamples*2)
	}
	for i := range samples {
		if got[i] != samples[i] {
			t.Fatalf("sample %d = %d, want %d", i, got[i], samples[i])
		}
	}
}

func TestInjectSendsPCM16FramesUntilContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	ready := make(chan struct{})
	close(ready)
	source := []int16{1000, -1000, 500, -500}
	messages := make(chan wsMessage, 4)
	logFile, err := os.Create(filepath.Join(t.TempDir(), "lifecycle.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()
	l := &logger{enc: json.NewEncoder(logFile)}
	go inject(ctx, ready, func(message wsMessage) error {
		messages <- message
		cancel()
		return nil
	}, source, 0, l)
	select {
	case message := <-messages:
		if message.Event != "media" || message.Track != "outbound" || message.SampleRate != rate {
			t.Fatalf("unexpected media message: %#v", message)
		}
		raw, err := base64.StdEncoding.DecodeString(message.Payload)
		if err != nil {
			t.Fatal(err)
		}
		if len(raw) != frameBytes {
			t.Fatalf("payload bytes = %d, want %d", len(raw), frameBytes)
		}
		first := int16(binary.LittleEndian.Uint16(raw[:2]))
		if first != 1000 {
			t.Fatalf("first sample = %d, want source sample", first)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for injected frame")
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
