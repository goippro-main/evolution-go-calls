package call_stream

import (
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/purpshell/meowcaller"
)

const testSigningKey = "test-signing-key-with-at-least-32-chars"

type fakeStreamInstanceResolver struct {
	byToken     map[string]*instance_model.Instance
	byID        map[string]*instance_model.Instance
	tokenLookup string
	idLookup    string
}

func (f *fakeStreamInstanceResolver) GetInstanceByToken(token string) (*instance_model.Instance, error) {
	f.tokenLookup = token
	if instance, ok := f.byToken[token]; ok {
		return instance, nil
	}
	return nil, errors.New("instance not found")
}

func (f *fakeStreamInstanceResolver) Info(instanceID string) (*instance_model.Instance, error) {
	f.idLookup = instanceID
	if instance, ok := f.byID[instanceID]; ok {
		return instance, nil
	}
	return nil, errors.New("instance not found")
}

func TestStreamAuthSignedTokenVector(t *testing.T) {
	t.Setenv(streamSigningKeyEnv, testSigningKey)
	resolver := &fakeStreamInstanceResolver{byID: map[string]*instance_model.Instance{
		"rekovi": {Id: "rekovi"},
	}}

	response := runStreamAuthRequest(
		t,
		resolver,
		time.Unix(1_799_999_940, 0),
		"/call/stream/ABC123?instance=rekovi&exp=1800000000&token=b3cc9b81c9d31c8efd991e92f91c0821c6b7be242d925676210ca6ab76c7072b",
	)

	if response.Code != http.StatusNoContent {
		t.Fatalf("expected signed token to pass, got status %d", response.Code)
	}
	if resolver.idLookup != "rekovi" {
		t.Fatalf("expected instance lookup for rekovi, got %q", resolver.idLookup)
	}
}

func TestStreamAuthRejectsExpiredToken(t *testing.T) {
	t.Setenv(streamSigningKeyEnv, testSigningKey)
	resolver := &fakeStreamInstanceResolver{byID: map[string]*instance_model.Instance{
		"rekovi": {Id: "rekovi"},
	}}

	response := runStreamAuthRequest(
		t,
		resolver,
		time.Unix(1_800_000_006, 0),
		"/call/stream/ABC123?instance=rekovi&exp=1800000000&token=b3cc9b81c9d31c8efd991e92f91c0821c6b7be242d925676210ca6ab76c7072b",
	)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("expected expired token to be rejected, got status %d", response.Code)
	}
	if resolver.idLookup != "" {
		t.Fatalf("expired token must be rejected before instance lookup, got %q", resolver.idLookup)
	}
}

func TestStreamAuthAllowsFiveSecondClockSkew(t *testing.T) {
	t.Setenv(streamSigningKeyEnv, testSigningKey)
	resolver := &fakeStreamInstanceResolver{byID: map[string]*instance_model.Instance{
		"rekovi": {Id: "rekovi"},
	}}

	response := runStreamAuthRequest(
		t,
		resolver,
		time.Unix(1_800_000_005, 0),
		"/call/stream/ABC123?instance=rekovi&exp=1800000000&token=b3cc9b81c9d31c8efd991e92f91c0821c6b7be242d925676210ca6ab76c7072b",
	)

	if response.Code != http.StatusNoContent {
		t.Fatalf("expected five-second clock skew to pass, got status %d", response.Code)
	}
}

func TestStreamAuthRejectsTokenTooFarInFuture(t *testing.T) {
	t.Setenv(streamSigningKeyEnv, testSigningKey)
	resolver := &fakeStreamInstanceResolver{byID: map[string]*instance_model.Instance{
		"rekovi": {Id: "rekovi"},
	}}

	response := runStreamAuthRequest(
		t,
		resolver,
		time.Unix(1_800_000_000, 0),
		"/call/stream/ABC123?instance=rekovi&exp=1800000401&token=does-not-matter",
	)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("expected far-future token to be rejected, got status %d", response.Code)
	}
	if resolver.idLookup != "" {
		t.Fatal("far-future token must be rejected before instance lookup")
	}
}

func TestStreamAuthRejectsWrongSignature(t *testing.T) {
	t.Setenv(streamSigningKeyEnv, testSigningKey)
	resolver := &fakeStreamInstanceResolver{byID: map[string]*instance_model.Instance{
		"rekovi": {Id: "rekovi"},
	}}

	response := runStreamAuthRequest(
		t,
		resolver,
		time.Unix(1_799_999_940, 0),
		"/call/stream/ABC123?instance=rekovi&exp=1800000000&token=a3cc9b81c9d31c8efd991e92f91c0821c6b7be242d925676210ca6ab76c7072b",
	)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("expected wrong signature to be rejected, got status %d", response.Code)
	}
	if resolver.idLookup != "" {
		t.Fatalf("wrong signature must be rejected before instance lookup, got %q", resolver.idLookup)
	}
}

func TestStreamAuthRejectsDivergentInstance(t *testing.T) {
	t.Setenv(streamSigningKeyEnv, testSigningKey)
	resolver := &fakeStreamInstanceResolver{byID: map[string]*instance_model.Instance{
		"other": {Id: "other"},
	}}

	response := runStreamAuthRequest(
		t,
		resolver,
		time.Unix(1_799_999_940, 0),
		"/call/stream/ABC123?instance=other&exp=1800000000&token=b3cc9b81c9d31c8efd991e92f91c0821c6b7be242d925676210ca6ab76c7072b",
	)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("expected token signed for another instance to be rejected, got status %d", response.Code)
	}
	if resolver.idLookup != "" {
		t.Fatalf("divergent instance must be rejected before instance lookup, got %q", resolver.idLookup)
	}
}

func TestStreamAuthKeepsAPIKeyPath(t *testing.T) {
	t.Setenv(streamSigningKeyEnv, "")
	resolver := &fakeStreamInstanceResolver{byToken: map[string]*instance_model.Instance{
		"legacy-instance-token": {Id: "rekovi"},
	}}

	response := runStreamAuthRequest(
		t,
		resolver,
		time.Unix(1_800_000_000, 0),
		"/call/stream/ABC123?apikey=legacy-instance-token",
	)

	if response.Code != http.StatusNoContent {
		t.Fatalf("expected apikey authentication to pass, got status %d", response.Code)
	}
	if resolver.tokenLookup != "legacy-instance-token" {
		t.Fatalf("expected apikey lookup, got %q", resolver.tokenLookup)
	}
	if resolver.idLookup != "" {
		t.Fatalf("apikey path must not resolve by instance id, got %q", resolver.idLookup)
	}
}

func runStreamAuthRequest(
	t *testing.T,
	resolver streamInstanceResolver,
	now time.Time,
	target string,
) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.GET("/call/stream/:callId", streamAuthMiddleware(resolver, func() time.Time { return now }), func(ctx *gin.Context) {
		if _, exists := ctx.Get(streamInstanceContextKey); !exists {
			ctx.AbortWithStatus(http.StatusInternalServerError)
			return
		}
		ctx.Status(http.StatusNoContent)
	})

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, target, nil)
	router.ServeHTTP(response, request)
	return response
}

func TestBridgeDropsInboundUntilCallIsActive(t *testing.T) {
	bridge := newBridge(nil)
	if err := bridge.WriteFrame(make([]float32, 960)); err != nil {
		t.Fatalf("expected pre-accept audio to be dropped, got %v", err)
	}
}

func TestBridgeDoesNotReleaseOutboundAudioBeforeActive(t *testing.T) {
	bridge := newBridge(nil)
	frame, err := bridge.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	if len(frame) != meowcaller.FrameSamples {
		t.Fatalf("pre-active frame has %d samples", len(frame))
	}
	for i, sample := range frame {
		if sample != 0 {
			t.Fatalf("pre-active sample %d is %v, want silence", i, sample)
		}
	}

	bridge.allowInbound()
	bridge.incoming <- make([]float32, meowcaller.FrameSamples)
	frame, err = bridge.ReadFrame()
	if err != nil {
		t.Fatalf("expected frame after activation, got %v", err)
	}
	if len(frame) != meowcaller.FrameSamples {
		t.Fatalf("active frame has %d samples", len(frame))
	}
}

func TestBridgeDiagnosticsRequireBidirectionalNonZeroFrames(t *testing.T) {
	bridge := newBridge(nil)
	bridge.allowInbound()
	inbound := make([]float32, meowcaller.FrameSamples)
	inbound[0] = 0.5
	outbound := make([]float32, meowcaller.FrameSamples)
	outbound[1] = -0.25
	bridge.recordInbound(inbound)
	bridge.incoming <- outbound
	if _, err := bridge.ReadFrame(); err != nil {
		t.Fatal(err)
	}

	stats := bridge.diagnostics()
	if stats["inboundFrames"] != int64(1) || stats["inboundNonZeroFrames"] != int64(1) {
		t.Fatalf("unexpected inbound diagnostics: %+v", stats)
	}
	if stats["outboundFrames"] != int64(1) || stats["outboundNonZeroFrames"] != int64(1) {
		t.Fatalf("unexpected outbound diagnostics: %+v", stats)
	}
	if stats["bidirectionalMedia"] != true {
		t.Fatalf("expected bidirectional media evidence: %+v", stats)
	}
}

func TestFrameStatsTreatsSilenceAsZero(t *testing.T) {
	nonZeroFrames, nonZeroSamples, peak := frameStats(make([]float32, meowcaller.FrameSamples))
	if nonZeroFrames != 0 || nonZeroSamples != 0 || peak != 0 {
		t.Fatalf("silence stats = %d/%d/%d", nonZeroFrames, nonZeroSamples, peak)
	}
}

func TestDecodeOutboundAudioRejectsMalformedMessages(t *testing.T) {
	valid := base64.StdEncoding.EncodeToString(make([]byte, meowcaller.FrameSamples*2))
	tests := []struct {
		name string
		msg  wsMessage
	}{
		{name: "empty payload", msg: wsMessage{Event: "media", Track: "outbound"}},
		{name: "bad base64", msg: wsMessage{Event: "media", Track: "outbound", Payload: "%%%"}},
		{name: "odd pcm length", msg: wsMessage{Event: "media", Track: "outbound", Payload: base64.StdEncoding.EncodeToString([]byte{1})}},
		{name: "short frame", msg: wsMessage{Event: "media", Track: "outbound", Payload: base64.StdEncoding.EncodeToString(make([]byte, 10))}},
		{name: "wrong sample rate", msg: wsMessage{Event: "media", Track: "outbound", SampleRate: 8000, Payload: valid}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := decodeOutboundAudio(tt.msg); err == nil {
				t.Fatal("expected malformed message to be rejected")
			}
		})
	}
}

func TestBridgeWritesInboundPCMOverWebSocket(t *testing.T) {
	serverResult := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			serverResult <- err
			return
		}
		bridge := newBridge(conn)
		bridge.allowInbound()
		frame := make([]float32, meowcaller.FrameSamples)
		frame[0] = 0.5
		serverResult <- bridge.WriteFrame(frame)
		_ = bridge.Close()
	}))
	defer server.Close()

	client, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	var msg wsMessage
	if err := client.ReadJSON(&msg); err != nil {
		t.Fatal(err)
	}
	if msg.Event != "media" || msg.Track != "inbound" {
		t.Fatalf("unexpected websocket message: %+v", msg)
	}
	raw, err := base64.StdEncoding.DecodeString(msg.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != meowcaller.FrameSamples*2 {
		t.Fatalf("got %d PCM bytes, want %d", len(raw), meowcaller.FrameSamples*2)
	}
	decoded := float32FromPCM16(raw)
	if decoded[0] < 0.49 || decoded[0] > 0.51 {
		t.Fatalf("unexpected first PCM sample %v", decoded[0])
	}
	if err := <-serverResult; err != nil {
		t.Fatal(err)
	}
}
