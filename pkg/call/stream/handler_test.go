package call_stream

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	"github.com/gin-gonic/gin"
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
	if err := bridge.WriteVideo([]byte{0x00, 0x00, 0x01}); err != nil {
		t.Fatalf("expected pre-accept video to be dropped, got %v", err)
	}
}
