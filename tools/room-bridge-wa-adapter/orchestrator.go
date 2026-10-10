package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type orchestratorConfig struct {
	apiBaseURL         string
	instance           string
	apiKey             string
	signingKey         string
	webhookListen      string
	webhookURL         string
	rollbackWebhookURL string
	allowedCallersCSV  string
	configureCutover   bool
	offerTimeout       time.Duration
	callTimeLimit      time.Duration
	httpTimeout        time.Duration
	streamTokenTTL     time.Duration
	adapterConfig      config
	allowedCallers     map[string]struct{}
}

type inboundOffer struct {
	ID      string
	Creator string
}

type controlledOrchestrator struct {
	c      orchestratorConfig
	client *http.Client
	offers chan inboundOffer

	mu       sync.Mutex
	selected bool
}

func runControlledInboundOrchestrator(ctx context.Context, c orchestratorConfig) error {
	c, err := validateOrchestratorConfig(c)
	if err != nil {
		return err
	}
	o := &controlledOrchestrator{
		c:      c,
		client: &http.Client{Timeout: c.httpTimeout},
		offers: make(chan inboundOffer, 1),
	}

	server := &http.Server{
		Addr:              c.webhookListen,
		ReadHeaderTimeout: 5 * time.Second,
		Handler:           http.HandlerFunc(o.handleWebhook),
	}
	listener, err := net.Listen("tcp", c.webhookListen)
	if err != nil {
		return fmt.Errorf("listen webhook: %w", err)
	}
	if o.c.webhookURL == "" {
		o.c.webhookURL = "http://" + listener.Addr().String() + "/webhook"
	}
	log.Printf("orchestrator listening webhook=%s roomBridge=%s", o.c.webhookURL, redactRawURL(o.c.adapterConfig.roomBridgeURL))
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("orchestrator webhook server stopped with error: %v", err)
		}
	}()
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = server.Shutdown(stopCtx)
	}()

	rollbackNeeded := false
	if o.c.configureCutover {
		if err := o.configureWebhook(ctx, o.c.webhookURL); err != nil {
			return fmt.Errorf("configure orchestrator webhook: %w", err)
		}
		rollbackNeeded = true
		log.Printf("orchestrator cutover configured for one allowlisted inbound call")
	}
	defer func() {
		if rollbackNeeded {
			if err := o.configureWebhook(context.Background(), o.c.rollbackWebhookURL); err != nil {
				log.Printf("orchestrator rollback failed: %v", err)
			} else {
				log.Printf("orchestrator rollback restored previous webhook")
			}
		}
	}()

	offerCtx, cancelOffer := context.WithTimeout(ctx, o.c.offerTimeout)
	defer cancelOffer()
	var offer inboundOffer
	select {
	case <-offerCtx.Done():
		return fmt.Errorf("wait for allowlisted inbound CallOffer: %w", offerCtx.Err())
	case offer = <-o.offers:
	}
	log.Printf("orchestrator accepted one inbound CallOffer callId=%s", offer.ID)

	if err := o.post(ctx, "/call/answer", map[string]string{"callId": offer.ID, "callCreator": offer.Creator}, nil); err != nil {
		return fmt.Errorf("answer inbound call: %w", err)
	}
	log.Printf("orchestrator answered callId=%s", offer.ID)

	streamURL, err := signedEvolutionStreamURL(o.c.apiBaseURL, o.c.instance, o.c.signingKey, offer.ID, o.c.streamTokenTTL, time.Now)
	if err != nil {
		_ = o.hangup(context.Background(), offer.ID)
		return err
	}
	adapterCfg := o.c.adapterConfig
	adapterCfg.evolutionURL = streamURL
	adapterCfg.callID = offer.ID
	adapterCfg.allowLegacyAPIKey = false

	callCtx, cancelCall := context.WithTimeout(ctx, o.c.callTimeLimit)
	defer cancelCall()
	errCh := make(chan error, 1)
	go func() {
		errCh <- run(callCtx, adapterCfg)
	}()

	var streamErr error
	hangupDone := false
	select {
	case streamErr = <-errCh:
	case <-callCtx.Done():
		streamErr = fmt.Errorf("call time limit elapsed: %w", callCtx.Err())
		if err := o.hangup(context.Background(), offer.ID); err != nil {
			log.Printf("orchestrator hangup after time limit failed callId=%s error=%v", offer.ID, err)
		} else {
			hangupDone = true
		}
		select {
		case <-errCh:
		case <-time.After(o.c.adapterConfig.readTimeout + time.Second):
			log.Printf("orchestrator adapter did not exit before grace deadline callId=%s", offer.ID)
		}
	}
	if !hangupDone {
		if err := o.hangup(context.Background(), offer.ID); err != nil {
			if streamErr == nil {
				streamErr = fmt.Errorf("hangup call: %w", err)
			} else {
				log.Printf("orchestrator hangup failed callId=%s error=%v", offer.ID, err)
			}
		}
	}
	if streamErr != nil {
		return streamErr
	}
	log.Printf("orchestrator completed one-call bridge callId=%s", offer.ID)
	return nil
}

func validateOrchestratorConfig(c orchestratorConfig) (orchestratorConfig, error) {
	if c.apiBaseURL == "" || c.instance == "" || c.apiKey == "" || c.signingKey == "" {
		return c, errors.New("orchestrator requires -api, -instance, -apikey, and -signing-key")
	}
	apiURL, err := url.Parse(c.apiBaseURL)
	if err != nil {
		return c, fmt.Errorf("api URL: %w", err)
	}
	if apiURL.Scheme != "http" && apiURL.Scheme != "https" {
		return c, errors.New("api URL must use http or https")
	}
	if apiURL.Hostname() == "" || apiURL.Fragment != "" {
		return c, errors.New("api URL must include host and no fragment")
	}
	apiURL.RawQuery = ""
	c.apiBaseURL = strings.TrimRight(apiURL.String(), "/")
	if c.adapterConfig.roomBridgeURL == "" {
		return c, errors.New("orchestrator requires -room-bridge-ws-url")
	}
	roomURL, err := parseWSURL(c.adapterConfig.roomBridgeURL)
	if err != nil {
		return c, fmt.Errorf("room_bridge URL: %w", err)
	}
	if !isLoopbackHost(roomURL.Hostname()) {
		return c, fmt.Errorf("room_bridge URL must be loopback/private tunnel, got host %q", roomURL.Hostname())
	}
	if c.configureCutover && c.rollbackWebhookURL == "" {
		return c, errors.New("-rollback-webhook-url is required with -configure-cutover")
	}
	if c.offerTimeout <= 0 || c.callTimeLimit <= 0 || c.httpTimeout <= 0 || c.streamTokenTTL <= 0 {
		return c, errors.New("orchestrator timeouts and stream token TTL must be positive")
	}
	if c.adapterConfig.maxQueuedFrames <= 0 {
		return c, errors.New("-max-queued-frames must be positive")
	}
	if c.adapterConfig.maxRoomReconnects < 0 {
		return c, errors.New("-max-room-reconnects must be non-negative")
	}
	if c.adapterConfig.lockDir == "" {
		return c, errors.New("-lock-dir is required so the orchestrator can enforce one local stream owner")
	}
	c.allowedCallers = parseAllowlist(c.allowedCallersCSV)
	if len(c.allowedCallers) == 0 {
		return c, errors.New("-allow-caller must name at least one allowed inbound CallCreator")
	}
	return c, nil
}

func parseAllowlist(raw string) map[string]struct{} {
	out := make(map[string]struct{})
	for _, item := range strings.Split(raw, ",") {
		item = normalizeCaller(item)
		if item != "" {
			out[item] = struct{}{}
		}
	}
	return out
}

func normalizeCaller(raw string) string {
	return strings.ToLower(strings.TrimSpace(raw))
}

func (o *controlledOrchestrator) handleWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()
	var payload map[string]any
	if err := json.NewDecoder(io.LimitReader(r.Body, 2<<20)).Decode(&payload); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		log.Printf("orchestrator ignored invalid webhook JSON: %v", err)
		return
	}
	event, _ := findString(payload, "event")
	if !strings.EqualFold(event, "CallOffer") {
		w.WriteHeader(http.StatusOK)
		return
	}
	offer := inboundOffer{
		ID:      firstString(payload, "CallID", "callId", "callID"),
		Creator: firstString(payload, "CallCreator", "callCreator", "from", "From"),
	}
	if webhookOfferEnded(payload) {
		log.Printf("orchestrator ignored ended CallOffer callId=%s", offer.ID)
		w.WriteHeader(http.StatusOK)
		return
	}
	if offer.ID == "" || offer.Creator == "" {
		log.Printf("orchestrator ignored malformed CallOffer")
		w.WriteHeader(http.StatusOK)
		return
	}
	if _, ok := o.c.allowedCallers[normalizeCaller(offer.Creator)]; !ok {
		log.Printf("orchestrator ignored non-allowlisted CallOffer callId=%s", offer.ID)
		w.WriteHeader(http.StatusOK)
		return
	}
	o.mu.Lock()
	if o.selected {
		o.mu.Unlock()
		log.Printf("orchestrator ignored extra CallOffer callId=%s", offer.ID)
		w.WriteHeader(http.StatusOK)
		return
	}
	o.selected = true
	o.mu.Unlock()
	select {
	case o.offers <- offer:
	default:
	}
	w.WriteHeader(http.StatusOK)
}

func webhookOfferEnded(payload map[string]any) bool {
	if value, ok := findString(payload, "is_call_ended"); ok {
		switch strings.ToLower(strings.TrimSpace(value)) {
		case "1", "true", "yes":
			return true
		}
	}
	if value, ok := findString(payload, "terminate_reason"); ok && strings.TrimSpace(value) != "" {
		return true
	}
	return false
}

func firstString(root map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := findString(root, key); ok && value != "" {
			return value
		}
	}
	return ""
}

func findString(value any, wanted string) (string, bool) {
	switch current := value.(type) {
	case map[string]any:
		for key, item := range current {
			if strings.EqualFold(key, wanted) {
				switch typed := item.(type) {
				case string:
					return typed, true
				case json.Number:
					return typed.String(), true
				}
			}
		}
		for _, item := range current {
			if text, ok := findString(item, wanted); ok {
				return text, true
			}
		}
	case []any:
		for _, item := range current {
			if text, ok := findString(item, wanted); ok {
				return text, true
			}
		}
	}
	return "", false
}

func (o *controlledOrchestrator) configureWebhook(ctx context.Context, webhook string) error {
	return o.post(ctx, "/instance/connect", map[string]any{
		"webhookUrl": webhook,
		"subscribe":  []string{"CALL"},
		"immediate":  true,
	}, nil)
}

func (o *controlledOrchestrator) hangup(ctx context.Context, callID string) error {
	err := o.post(ctx, "/call/hangup", map[string]string{"callId": callID}, nil)
	if err == nil {
		log.Printf("orchestrator hangup sent callId=%s", callID)
	}
	return err
}

func (o *controlledOrchestrator) post(ctx context.Context, path string, body any, result any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, o.c.apiBaseURL+path, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("apikey", o.c.apiKey)
	resp, err := o.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%s: %s: %s", path, resp.Status, strings.TrimSpace(string(bodyBytes)))
	}
	if result != nil && len(bodyBytes) > 0 {
		if err := json.Unmarshal(bodyBytes, result); err != nil {
			return fmt.Errorf("%s: invalid JSON response: %w", path, err)
		}
	}
	return nil
}

func signedEvolutionStreamURL(apiBase, instance, signingKey, callID string, ttl time.Duration, now func() time.Time) (string, error) {
	u, err := url.Parse(apiBase)
	if err != nil {
		return "", err
	}
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	default:
		return "", errors.New("api URL must use http or https")
	}
	exp := now().Add(ttl).Unix()
	message := fmt.Sprintf("%s|%s|%d", instance, callID, exp)
	mac := hmac.New(sha256.New, []byte(signingKey))
	_, _ = mac.Write([]byte(message))
	u.Path = "/call/stream/" + url.PathEscape(callID)
	q := u.Query()
	q.Set("instance", instance)
	q.Set("exp", fmt.Sprint(exp))
	q.Set("token", hex.EncodeToString(mac.Sum(nil)))
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func redactRawURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "<invalid-url>"
	}
	return redactURL(u)
}
