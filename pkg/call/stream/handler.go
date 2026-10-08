package call_stream

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	call_service "github.com/evolution-foundation/evolution-go/pkg/call/service"
	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	instance_service "github.com/evolution-foundation/evolution-go/pkg/instance/service"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/purpshell/meowcaller"
)

const (
	streamInstanceContextKey = "callStreamInstance"
	streamSigningKeyEnv      = "CALL_STREAM_SIGNING_KEY"
	streamClockSkewSeconds   = int64(5)
	streamMaxTokenLifetime   = int64(5 * 60)
)

var errStreamUnauthorized = errors.New("not authorized")

type streamInstanceResolver interface {
	GetInstanceByToken(token string) (*instance_model.Instance, error)
	Info(instanceID string) (*instance_model.Instance, error)
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 4096,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

// RegisterRoutes mounts the stream route outside the normal header-auth group.
// HMAC URLs are the preferred browser/operator mechanism; the apikey path is
// retained only for backwards compatibility with existing non-browser clients.
func RegisterRoutes(r *gin.Engine, callService call_service.CallService, instanceService instance_service.InstanceService) {
	r.GET("/call/stream/:callId", streamAuthMiddleware(instanceService, time.Now), serveStream(callService))
}

func streamAuthMiddleware(instanceService streamInstanceResolver, now func() time.Time) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		instance, err := authenticateStream(ctx, instanceService, now())
		if err != nil {
			ctx.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "not authorized"})
			return
		}
		ctx.Set(streamInstanceContextKey, instance)
		ctx.Next()
	}
}

func authenticateStream(ctx *gin.Context, instanceService streamInstanceResolver, now time.Time) (*instance_model.Instance, error) {
	if apiKey := ctx.Query("apikey"); apiKey != "" {
		return instanceService.GetInstanceByToken(apiKey)
	}

	instanceID := ctx.Query("instance")
	if instanceID == "" {
		return nil, errStreamUnauthorized
	}
	exp, err := strconv.ParseInt(ctx.Query("exp"), 10, 64)
	if err != nil {
		return nil, errStreamUnauthorized
	}
	nowUnix := now.Unix()
	if exp < nowUnix-streamClockSkewSeconds || exp > nowUnix+streamMaxTokenLifetime {
		return nil, errStreamUnauthorized
	}

	key := os.Getenv(streamSigningKeyEnv)
	if key == "" {
		return nil, errStreamUnauthorized
	}

	message := fmt.Sprintf("%s|%s|%d", instanceID, ctx.Param("callId"), exp)
	mac := hmac.New(sha256.New, []byte(key))
	_, _ = mac.Write([]byte(message))
	expected := mac.Sum(nil)
	provided, err := hex.DecodeString(ctx.Query("token"))
	if err != nil || !hmac.Equal(provided, expected) {
		return nil, errStreamUnauthorized
	}

	instance, err := instanceService.Info(instanceID)
	if err != nil || instance == nil || instance.Id != instanceID {
		return nil, errStreamUnauthorized
	}
	return instance, nil
}

func serveStream(callService call_service.CallService) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		instanceValue, ok := ctx.Get(streamInstanceContextKey)
		instance, valid := instanceValue.(*instance_model.Instance)
		if !ok || !valid {
			ctx.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "not authorized"})
			return
		}

		callID := ctx.Param("callId")
		call, err := callService.GetActiveCall(instance.Id, callID)
		if err != nil {
			ctx.AbortWithStatusJSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}

		conn, err := upgrader.Upgrade(ctx.Writer, ctx.Request, nil)
		if err != nil {
			return
		}

		b := newBridge(conn)
		outgoing := callService.IsOutgoingCall(instance.Id, callID)
		defer func() {
			_ = b.Close()
		}()

		call.OnStateChange(func(phase meowcaller.CallPhase) {
			if phase == meowcaller.CallPhaseActive {
				b.allowInbound()
				b.allowOutbound()
			}
			_ = b.writeState(callID, callPhaseName(phase), callDirectionName(outgoing))
			if phase == meowcaller.CallPhaseEnded {
				_ = b.Close()
			}
		})
		if outgoing {
			// Outbound media is allowed as soon as the peer accepts. This can
			// precede CallPhaseActive, which waits for the first RTP exchange.
			call.OnPeerAccept(func() {
				b.allowInbound()
				b.allowOutbound()
				_ = b.writeJSON(wsMessage{Event: "peer_accept", CallID: callID, Direction: "outgoing"})
			})
		} else if call.State() == meowcaller.CallPhaseActive {
			b.allowInbound()
			b.allowOutbound()
		} else {
			// For answered inbound calls, operator/AI audio must be allowed before
			// CallPhaseActive. Active is only reached after inbound RTP is decoded,
			// and waiting for it can deadlock "callee speaks first" flows into silence.
			b.allowOutbound()
		}
		if call.State() == meowcaller.CallPhaseEnded {
			return
		}

		call.OnEnd(func(reason string) { _ = b.Close() })
		call.Receive(b)
		call.Play(b)
		_ = b.writeState(callID, callPhaseName(call.State()), callDirectionName(outgoing))
		if err := b.writeStart(callID); err != nil {
			return
		}
		if !outgoing {
			_ = b.writeJSON(wsMessage{Event: "media_ready", CallID: callID, Direction: "incoming"})
		}
		_ = b.readLoop()
	}
}

func callDirectionName(outgoing bool) string {
	if outgoing {
		return "outgoing"
	}
	return "incoming"
}

func callPhaseName(phase meowcaller.CallPhase) string {
	switch phase {
	case meowcaller.CallPhaseIdle:
		return "idle"
	case meowcaller.CallPhaseCalling:
		return "calling"
	case meowcaller.CallPhaseRinging:
		return "ringing"
	case meowcaller.CallPhaseConnecting:
		return "connecting"
	case meowcaller.CallPhaseActive:
		return "active"
	case meowcaller.CallPhaseEnded:
		return "ended"
	default:
		return "unknown"
	}
}
