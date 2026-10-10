package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSignedEvolutionStreamURLUsesBoundedHMAC(t *testing.T) {
	now := func() time.Time { return time.Unix(1000, 0) }
	got, err := signedEvolutionStreamURL("http://127.0.0.1:4000/base", "sala2", "signing-secret", "call-1", 2*time.Minute, now)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(got)
	if err != nil {
		t.Fatal(err)
	}
	if u.Scheme != "ws" || u.Path != "/call/stream/call-1" {
		t.Fatalf("bad stream URL: %s", got)
	}
	if u.Query().Get("apikey") != "" {
		t.Fatalf("stream URL must not use apikey: %s", got)
	}
	if u.Query().Get("instance") != "sala2" || u.Query().Get("exp") != "1120" {
		t.Fatalf("bad query: %v", u.Query())
	}
	mac := hmac.New(sha256.New, []byte("signing-secret"))
	_, _ = mac.Write([]byte("sala2|call-1|1120"))
	if want := hex.EncodeToString(mac.Sum(nil)); u.Query().Get("token") != want {
		t.Fatalf("token = %q, want %q", u.Query().Get("token"), want)
	}
}

func TestControlledInboundOrchestratorBridgesOneAllowlistedCallAndRollsBack(t *testing.T) {
	var logBuf bytes.Buffer
	oldLog := log.Writer()
	log.SetOutput(&logBuf)
	defer log.SetOutput(oldLog)

	inboundSpeech := frameWithSample(1400)
	outbound := frameWithSample(-1800)
	roomConnected := make(chan struct{}, 1)
	roomSawMedia := make(chan roomMessage, 1)
	roomServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		roomConnected <- struct{}{}
		if got := r.URL.Query().Get("room"); got != "wa-call-call-1" {
			t.Errorf("room query = %q", got)
		}
		conn, err := testUpgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("room upgrade: %v", err)
			return
		}
		defer conn.Close()
		var start roomMessage
		if err := readJSON(conn, time.Second, &start); err != nil {
			t.Errorf("room start: %v", err)
			return
		}
		if start.Event != "start" || start.Start == nil || start.Start.CallSid != "call-1" {
			t.Errorf("bad room start: %#v", start)
			return
		}
		var media roomMessage
		if err := readJSON(conn, time.Second, &media); err != nil {
			t.Errorf("room media: %v", err)
			return
		}
		roomSawMedia <- media
		_ = writeJSON(conn, time.Second, roomMessage{Event: "media", Media: &roomMedia{Track: "outbound", Payload: base64.StdEncoding.EncodeToString(outbound)}})
		var stop roomMessage
		_ = readJSON(conn, time.Second, &stop)
	}))
	defer roomServer.Close()

	state := newMockEvolutionState(t, "instance-secret", "sala2", "signing-secret")
	evoServer := httptest.NewServer(http.HandlerFunc(state.handle))
	defer evoServer.Close()

	err := runControlledInboundOrchestrator(t.Context(), orchestratorConfig{
		apiBaseURL:            evoServer.URL,
		instance:              "sala2",
		apiKey:                "instance-secret",
		signingKey:            "signing-secret",
		webhookListen:         "127.0.0.1:0",
		rollbackWebhookURL:    "http://127.0.0.1:8090/webhook",
		allowedCallersCSV:     "351900000001@s.whatsapp.net",
		configureCutover:      true,
		rollbackWatchdogAfter: time.Minute,
		watchdogStarter:       noOpRollbackWatchdogStarter,
		offerTimeout:          2 * time.Second,
		callTimeLimit:         3 * time.Second,
		httpTimeout:           time.Second,
		streamTokenTTL:        time.Minute,
		adapterConfig: config{
			roomBridgeURL:     wsURLFromHTTP(roomServer.URL),
			dir:               "pt2ru",
			roomPrefix:        "wa-call-",
			lockDir:           t.TempDir(),
			readTimeout:       500 * time.Millisecond,
			writeTimeout:      time.Second,
			pingInterval:      0,
			vadThreshold:      300,
			prespeechFrames:   0,
			maxQueuedFrames:   4,
			maxRoomReconnects: 0,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	state.assertCounts(t, 2, 1, 1, 1)
	if state.dial.Load() != 0 {
		t.Fatalf("orchestrator must never dial; dial calls = %d", state.dial.Load())
	}
	if got := state.connectBodies[0]["webhookUrl"]; got == "http://127.0.0.1:8090/webhook" {
		t.Fatalf("first connect unexpectedly used rollback webhook")
	}
	if got := state.connectBodies[1]["webhookUrl"]; got != "http://127.0.0.1:8090/webhook" {
		t.Fatalf("rollback webhook = %v", got)
	}
	select {
	case <-roomConnected:
	default:
		t.Fatal("room bridge was not connected after speech")
	}
	select {
	case media := <-roomSawMedia:
		raw, err := base64.StdEncoding.DecodeString(media.Media.Payload)
		if err != nil {
			t.Fatal(err)
		}
		if string(raw) != string(inboundSpeech) {
			t.Fatal("room bridge received wrong inbound speech frame")
		}
	case <-time.After(time.Second):
		t.Fatal("room bridge did not receive speech media")
	}
	if state.outboundFrames.Load() == 0 {
		t.Fatal("Evolution stream did not receive outbound room audio")
	}
	logs := logBuf.String()
	for _, secret := range []string{"instance-secret", "signing-secret"} {
		if strings.Contains(logs, secret) {
			t.Fatalf("logs leaked %q: %s", secret, logs)
		}
	}
}

func TestControlledInboundOrchestratorIgnoresNonAllowlistedOfferAndRollsBack(t *testing.T) {
	state := newMockEvolutionState(t, "instance-secret", "sala2", "signing-secret")
	state.offerCreator = "351900000999@s.whatsapp.net"
	evoServer := httptest.NewServer(http.HandlerFunc(state.handle))
	defer evoServer.Close()

	err := runControlledInboundOrchestrator(t.Context(), orchestratorConfig{
		apiBaseURL:            evoServer.URL,
		instance:              "sala2",
		apiKey:                "instance-secret",
		signingKey:            "signing-secret",
		webhookListen:         "127.0.0.1:0",
		rollbackWebhookURL:    "http://127.0.0.1:8090/webhook",
		allowedCallersCSV:     "351900000001@s.whatsapp.net",
		configureCutover:      true,
		rollbackWatchdogAfter: time.Minute,
		watchdogStarter:       noOpRollbackWatchdogStarter,
		offerTimeout:          100 * time.Millisecond,
		callTimeLimit:         time.Second,
		httpTimeout:           time.Second,
		streamTokenTTL:        time.Minute,
		adapterConfig:         validOrchestratorAdapterConfig(t, "ws://127.0.0.1:8191"),
	})
	if err == nil || !strings.Contains(err.Error(), "wait for allowlisted inbound CallOffer") {
		t.Fatalf("expected allowlist timeout, got %v", err)
	}
	state.assertCounts(t, 2, 0, 0, 0)
}

func TestControlledInboundOrchestratorAnswerFailureRollsBackWithoutStream(t *testing.T) {
	state := newMockEvolutionState(t, "instance-secret", "sala2", "signing-secret")
	state.answerStatus.Store(http.StatusInternalServerError)
	evoServer := httptest.NewServer(http.HandlerFunc(state.handle))
	defer evoServer.Close()

	err := runControlledInboundOrchestrator(t.Context(), orchestratorConfig{
		apiBaseURL:            evoServer.URL,
		instance:              "sala2",
		apiKey:                "instance-secret",
		signingKey:            "signing-secret",
		webhookListen:         "127.0.0.1:0",
		rollbackWebhookURL:    "http://127.0.0.1:8090/webhook",
		allowedCallersCSV:     "351900000001@s.whatsapp.net",
		configureCutover:      true,
		rollbackWatchdogAfter: time.Minute,
		watchdogStarter:       noOpRollbackWatchdogStarter,
		offerTimeout:          time.Second,
		callTimeLimit:         time.Second,
		httpTimeout:           time.Second,
		streamTokenTTL:        time.Minute,
		adapterConfig:         validOrchestratorAdapterConfig(t, "ws://127.0.0.1:8191"),
	})
	if err == nil || !strings.Contains(err.Error(), "answer inbound call") {
		t.Fatalf("expected answer failure, got %v", err)
	}
	state.assertCounts(t, 2, 1, 0, 0)
}

func TestControlledInboundOrchestratorRoomFailureHangsUpAndRollsBack(t *testing.T) {
	roomServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := testUpgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("room upgrade: %v", err)
			return
		}
		_ = conn.Close()
	}))
	defer roomServer.Close()

	state := newMockEvolutionState(t, "instance-secret", "sala2", "signing-secret")
	evoServer := httptest.NewServer(http.HandlerFunc(state.handle))
	defer evoServer.Close()

	err := runControlledInboundOrchestrator(t.Context(), orchestratorConfig{
		apiBaseURL:            evoServer.URL,
		instance:              "sala2",
		apiKey:                "instance-secret",
		signingKey:            "signing-secret",
		webhookListen:         "127.0.0.1:0",
		rollbackWebhookURL:    "http://127.0.0.1:8090/webhook",
		allowedCallersCSV:     "351900000001@s.whatsapp.net",
		configureCutover:      true,
		rollbackWatchdogAfter: time.Minute,
		watchdogStarter:       noOpRollbackWatchdogStarter,
		offerTimeout:          time.Second,
		callTimeLimit:         time.Second,
		httpTimeout:           time.Second,
		streamTokenTTL:        time.Minute,
		adapterConfig:         validOrchestratorAdapterConfig(t, wsURLFromHTTP(roomServer.URL)),
	})
	if err == nil {
		t.Fatal("expected room bridge failure")
	}
	state.assertCounts(t, 2, 1, 1, 1)
}

func TestValidateOrchestratorRequiresCrashRollbackForCutover(t *testing.T) {
	cfg := orchestratorConfig{
		apiBaseURL:         "http://127.0.0.1:4000",
		instance:           "sala2",
		apiKey:             "instance-secret",
		signingKey:         "signing-secret",
		rollbackWebhookURL: "http://127.0.0.1:8090/webhook",
		allowedCallersCSV:  "351900000001@s.whatsapp.net",
		configureCutover:   true,
		offerTimeout:       time.Second,
		callTimeLimit:      time.Second,
		httpTimeout:        time.Second,
		streamTokenTTL:     time.Minute,
		adapterConfig:      validOrchestratorAdapterConfig(t, "ws://127.0.0.1:8191"),
	}
	if _, err := validateOrchestratorConfig(cfg); err == nil || !strings.Contains(err.Error(), "crash-rollback") {
		t.Fatalf("expected crash rollback validation error, got %v", err)
	}
}

func TestRollbackWebhookPostAndStateFile(t *testing.T) {
	var gotAPIKey string
	var gotWebhook string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/instance/connect" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		gotAPIKey = r.Header.Get("apikey")
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		gotWebhook, _ = body["webhookUrl"].(string)
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	}))
	defer server.Close()

	statePath := t.TempDir() + "/rollback.json"
	state := rollbackWatchdogState{
		APIBaseURL:         server.URL,
		APIKey:             "instance-secret",
		RollbackWebhookURL: "http://127.0.0.1:8090/webhook",
		DeadlineUnixNano:   time.Now().UnixNano(),
		HTTPTimeoutMillis:  int64(time.Second / time.Millisecond),
	}
	raw, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(statePath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := readRollbackWatchdogState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := postRollbackWebhook(t.Context(), loaded); err != nil {
		t.Fatal(err)
	}
	if gotAPIKey != "instance-secret" {
		t.Fatalf("apikey not sent to rollback endpoint")
	}
	if gotWebhook != "http://127.0.0.1:8090/webhook" {
		t.Fatalf("webhook = %q", gotWebhook)
	}
}

func validOrchestratorAdapterConfig(t *testing.T, roomURL string) config {
	t.Helper()
	return config{
		roomBridgeURL:     roomURL,
		dir:               "pt2ru",
		roomPrefix:        "wa-call-",
		lockDir:           t.TempDir(),
		readTimeout:       200 * time.Millisecond,
		writeTimeout:      time.Second,
		pingInterval:      0,
		vadThreshold:      300,
		prespeechFrames:   0,
		maxQueuedFrames:   2,
		maxRoomReconnects: 0,
	}
}

func noOpRollbackWatchdogStarter(context.Context, orchestratorConfig) (func(bool), error) {
	return func(bool) {}, nil
}

type mockEvolutionState struct {
	t               *testing.T
	apiKey          string
	instance        string
	signingKey      string
	offerCreator    string
	connect         atomic.Int64
	answer          atomic.Int64
	hangup          atomic.Int64
	stream          atomic.Int64
	dial            atomic.Int64
	outboundFrames  atomic.Int64
	answerStatus    atomic.Int64
	mu              sync.Mutex
	connectBodies   []map[string]any
	webhookAttempts atomic.Int64
}

func newMockEvolutionState(t *testing.T, apiKey, instance, signingKey string) *mockEvolutionState {
	s := &mockEvolutionState{
		t:            t,
		apiKey:       apiKey,
		instance:     instance,
		signingKey:   signingKey,
		offerCreator: "351900000001@s.whatsapp.net",
	}
	s.answerStatus.Store(http.StatusOK)
	return s
}

func (s *mockEvolutionState) handle(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/call/dial" {
		s.dial.Add(1)
		http.Error(w, "dial disabled", http.StatusForbidden)
		return
	}
	if r.URL.Path == "/call/stream/call-1" {
		s.stream.Add(1)
		s.handleStream(w, r)
		return
	}
	if r.Header.Get("apikey") != s.apiKey {
		http.Error(w, "bad key", http.StatusUnauthorized)
		return
	}
	switch r.URL.Path {
	case "/instance/connect":
		s.connect.Add(1)
		var body map[string]any
		decodeTestJSON(s.t, r.Body, &body)
		s.mu.Lock()
		s.connectBodies = append(s.connectBodies, body)
		s.mu.Unlock()
		webhookURL, _ := body["webhookUrl"].(string)
		if strings.Contains(webhookURL, "/webhook") && !strings.Contains(webhookURL, ":8090/") {
			go s.sendOffer(webhookURL)
		}
		writeTestJSON(w, map[string]any{"message": "success"})
	case "/call/answer":
		s.answer.Add(1)
		if status := int(s.answerStatus.Load()); status != http.StatusOK {
			http.Error(w, "answer failed", status)
			return
		}
		var body map[string]string
		decodeTestJSON(s.t, r.Body, &body)
		if body["callId"] != "call-1" || body["callCreator"] != s.offerCreator {
			s.t.Errorf("bad answer body: %#v", body)
		}
		writeTestJSON(w, map[string]string{"message": "success"})
	case "/call/hangup":
		s.hangup.Add(1)
		writeTestJSON(w, map[string]string{"message": "success"})
	default:
		http.NotFound(w, r)
	}
}

func (s *mockEvolutionState) sendOffer(webhookURL string) {
	if s.webhookAttempts.Add(1) > 1 {
		return
	}
	time.Sleep(20 * time.Millisecond)
	raw, _ := json.Marshal(map[string]any{
		"data": map[string]any{
			"event":       "CallOffer",
			"callId":      "call-1",
			"callCreator": s.offerCreator,
		},
	})
	resp, err := http.Post(webhookURL, "application/json", bytes.NewReader(raw))
	if err != nil {
		s.t.Errorf("post webhook: %v", err)
		return
	}
	_ = resp.Body.Close()
}

func (s *mockEvolutionState) handleStream(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Get("apikey") != "" {
		s.t.Error("stream used apikey")
	}
	exp := r.URL.Query().Get("exp")
	mac := hmac.New(sha256.New, []byte(s.signingKey))
	_, _ = mac.Write([]byte(s.instance + "|call-1|" + exp))
	wantToken := hex.EncodeToString(mac.Sum(nil))
	if r.URL.Query().Get("instance") != s.instance || r.URL.Query().Get("token") != wantToken {
		s.t.Errorf("bad signed stream query: %v", r.URL.Query())
	}
	conn, err := testUpgrader.Upgrade(w, r, nil)
	if err != nil {
		s.t.Errorf("stream upgrade: %v", err)
		return
	}
	defer conn.Close()
	_ = writeJSON(conn, time.Second, evolutionMessage{Event: "start", CallID: "call-1", SampleRate: sampleRate, Direction: "incoming"})
	_ = writeJSON(conn, time.Second, evolutionMessage{Event: "media_ready", CallID: "call-1", Direction: "incoming"})
	_ = writeJSON(conn, time.Second, evolutionMessage{Event: "media", CallID: "call-1", SampleRate: sampleRate, Track: "inbound", Payload: base64.StdEncoding.EncodeToString(frameWithSample(0))})
	time.Sleep(100 * time.Millisecond)
	_ = writeJSON(conn, time.Second, evolutionMessage{Event: "media", CallID: "call-1", SampleRate: sampleRate, Track: "inbound", Payload: base64.StdEncoding.EncodeToString(frameWithSample(1400))})
	var outbound evolutionMessage
	if err := readJSON(conn, time.Second, &outbound); err == nil && outbound.Event == "media" && outbound.Track == "outbound" {
		s.outboundFrames.Add(1)
	}
	_ = writeJSON(conn, time.Second, evolutionMessage{Event: "stop", CallID: "call-1"})
}

func (s *mockEvolutionState) assertCounts(t *testing.T, connect, answer, hangup, stream int64) {
	t.Helper()
	if s.connect.Load() != connect {
		t.Fatalf("connect = %d, want %d", s.connect.Load(), connect)
	}
	if s.answer.Load() != answer {
		t.Fatalf("answer = %d, want %d", s.answer.Load(), answer)
	}
	if s.hangup.Load() != hangup {
		t.Fatalf("hangup = %d, want %d", s.hangup.Load(), hangup)
	}
	if s.stream.Load() != stream {
		t.Fatalf("stream = %d, want %d", s.stream.Load(), stream)
	}
}

func decodeTestJSON(t *testing.T, r io.Reader, out any) {
	t.Helper()
	defer func() {
		if closer, ok := r.(io.Closer); ok {
			_ = closer.Close()
		}
	}()
	if err := json.NewDecoder(io.LimitReader(r, 2<<20)).Decode(out); err != nil {
		t.Fatalf("decode JSON: %v", err)
	}
}

func writeTestJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
