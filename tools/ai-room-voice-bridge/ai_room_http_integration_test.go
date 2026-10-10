package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAIRoomHTTPAgentWithIsolatedAIRoomHarness(t *testing.T) {
	h := startIsolatedAIRoomHarness(t)

	directClient := &http.Client{Timeout: 2 * time.Second}
	raw, status := postJSON(t, directClient, h.baseURL+"/api/chat/send", `{"message":"before init"}`)
	if status != http.StatusBadRequest || !strings.Contains(raw, "session not initialized") {
		t.Fatalf("POST without Flask session status=%d body=%s", status, raw)
	}

	sessionFile := filepath.Join(t.TempDir(), "ai-room-cookies.json")
	agent := buildTestAIRoomHTTPAgent(t, h.baseURL, sessionFile, 2*time.Second)

	first, err := agent.Reply(context.Background(), ConversationTurn{
		Text:     "Need two SIP lines and WhatsApp voice",
		Language: "en",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(first, "AI Room test reply") || !strings.Contains(first, "Need two SIP lines") {
		t.Fatalf("unexpected first reply: %q", first)
	}

	second, err := agent.Reply(context.Background(), ConversationTurn{
		Text:     "Also remember my company is TestCo",
		Language: "en",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(second, "AI Room test reply") || !strings.Contains(second, "Need two SIP lines") {
		t.Fatalf("second reply did not include prior context: %q", second)
	}

	info, err := os.Stat(sessionFile)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("session file mode = %v, want 0600", info.Mode().Perm())
	}

	events := h.readEvents(t)
	chatIDs := eventValues(events, "save_message", "chat_id")
	if len(chatIDs) < 4 {
		t.Fatalf("expected client+agent messages, got events: %#v", events)
	}
	for _, id := range chatIDs[1:] {
		if id != chatIDs[0] {
			t.Fatalf("Flask session did not preserve room identity: %v", chatIDs)
		}
	}
	modelCalls := eventObjects(events, "model_request")
	if len(modelCalls) < 2 {
		t.Fatalf("expected at least two BrainAgent model requests, got %#v", events)
	}
	if !strings.Contains(fmt.Sprint(modelCalls[1]["messages"]), "Need two SIP lines") {
		t.Fatalf("second model request did not include stored context: %#v", modelCalls[1])
	}
}

func TestOfflineRunWithAIRoomHTTPHarnessWAVRoundTrip(t *testing.T) {
	h := startIsolatedAIRoomHarness(t)
	dir := t.TempDir()
	in := filepath.Join(dir, "input.wav")
	samples := make([]int16, frameSamples*2)
	for i := range samples {
		samples[i] = int16(750 + i%31)
	}
	if err := writePCM16MonoWAV(in, samples); err != nil {
		t.Fatal(err)
	}

	out := filepath.Join(dir, "out")
	err := run(context.Background(), config{
		inWAV:             in,
		outDir:            out,
		sttProvider:       "fixed",
		fixedTranscript:   "I need an isolated AI Room voice bridge test.",
		agentProvider:     "ai-room-http",
		aiRoomURL:         h.baseURL,
		aiRoomSiteKey:     "wa-bridge-test",
		aiRoomLanguage:    "en",
		aiRoomSessionFile: filepath.Join(dir, "cookies.json"),
		allowNetwork:      true,
		ttsProvider:       "tone",
		maxReplyChars:     220,
		sttTimeout:        2 * time.Second,
		agentTimeout:      2 * time.Second,
		ttsTimeout:        2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	replyRaw, err := os.ReadFile(filepath.Join(out, "reply.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(replyRaw), "AI Room test reply") {
		t.Fatalf("reply did not come through AI-Room harness: %s", replyRaw)
	}
	responseSamples, responseStats, err := readPCM16MonoWAV(filepath.Join(out, "response-16k-mono-pcm16-960frames.wav"))
	if err != nil {
		t.Fatal(err)
	}
	if len(responseSamples) == 0 || !responseStats.FrameAligned || responseStats.NonZeroSamples == 0 {
		t.Fatalf("bad response WAV stats: %#v", responseStats)
	}
	outboundFrames := countJSONLLines(t, filepath.Join(out, "outbound-media.jsonl"))
	if outboundFrames == 0 || outboundFrames != responseStats.Frames960 {
		t.Fatalf("outbound frames=%d response frames=%d", outboundFrames, responseStats.Frames960)
	}

	var summary runSummary
	readJSONFile(t, filepath.Join(out, "run.json"), &summary)
	if summary.Providers["agent"] != "ai-room-http:wa-bridge-test" {
		t.Fatalf("unexpected provider summary: %#v", summary.Providers)
	}
	if summary.InputAudio.SampleRate != sampleRate || summary.ResponseAudio.SampleRate != sampleRate {
		t.Fatalf("unexpected audio rates: input=%#v response=%#v", summary.InputAudio, summary.ResponseAudio)
	}
}

func TestAIRoomHTTPAgentHTTPErrorBoundaries(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
		want    string
	}{
		{
			name: "session init error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "no room", http.StatusNotFound)
			},
			want: "session init failed",
		},
		{
			name: "send invalid json",
			handler: func(w http.ResponseWriter, r *http.Request) {
				if strings.HasPrefix(r.URL.Path, "/chat/") {
					w.WriteHeader(http.StatusOK)
					return
				}
				_, _ = io.WriteString(w, "{")
			},
			want: "invalid JSON",
		},
		{
			name: "send ok false",
			handler: func(w http.ResponseWriter, r *http.Request) {
				if strings.HasPrefix(r.URL.Path, "/chat/") {
					w.WriteHeader(http.StatusOK)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"ok":false,"error":"agent unavailable"}`)
			},
			want: "ok=false",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(tc.handler)
			t.Cleanup(server.Close)
			agent := buildTestAIRoomHTTPAgent(t, server.URL, "", time.Second)
			_, err := agent.Reply(context.Background(), ConversationTurn{Text: "hello"})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestAIRoomHTTPAgentTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	agent := buildTestAIRoomHTTPAgent(t, server.URL, "", 25*time.Millisecond)
	_, err := agent.Reply(context.Background(), ConversationTurn{Text: "hello"})
	if err == nil || !strings.Contains(err.Error(), "session init failed") {
		t.Fatalf("expected timeout during session init, got %v", err)
	}
}

type isolatedAIRoomHarness struct {
	baseURL  string
	eventLog string
	cmd      *exec.Cmd
}

func startIsolatedAIRoomHarness(t *testing.T) isolatedAIRoomHarness {
	t.Helper()
	source := "/Users/valera/aicc-push/ai-room"
	if _, err := os.Stat(filepath.Join(source, "web", "chat_service.py")); err != nil {
		t.Skipf("AI-Room source is unavailable: %v", err)
	}
	dir := t.TempDir()
	script := filepath.Join(dir, "airoom_harness.py")
	eventLog := filepath.Join(dir, "events.jsonl")
	if err := os.WriteFile(script, []byte(aiRoomHarnessScript), 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("python3", script)
	cmd.Env = append(os.Environ(),
		"AI_ROOM_SOURCE="+source,
		"AI_ROOM_EVENT_LOG="+eventLog,
		"OPENAI_API_KEY=sk-isolated-test",
		"CHAT_SECRET_KEY=isolated-ai-room-test-secret",
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})

	ready := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "READY ") {
				ready <- strings.TrimSpace(strings.TrimPrefix(line, "READY "))
				return
			}
		}
		ready <- ""
	}()

	select {
	case baseURL := <-ready:
		if baseURL == "" {
			t.Fatalf("AI-Room harness exited before ready: %s", stderr.String())
		}
		return isolatedAIRoomHarness{baseURL: baseURL, eventLog: eventLog, cmd: cmd}
	case <-time.After(10 * time.Second):
		t.Fatalf("AI-Room harness did not become ready: %s", stderr.String())
	}
	return isolatedAIRoomHarness{}
}

func (h isolatedAIRoomHarness) readEvents(t *testing.T) []map[string]any {
	t.Helper()
	raw, err := os.ReadFile(h.eventLog)
	if err != nil {
		t.Fatal(err)
	}
	var events []map[string]any
	scanner := bufio.NewScanner(strings.NewReader(string(raw)))
	for scanner.Scan() {
		var event map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			t.Fatal(err)
		}
		events = append(events, event)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return events
}

func buildTestAIRoomHTTPAgent(t *testing.T, baseURL, sessionFile string, timeout time.Duration) *aiRoomHTTPAgent {
	t.Helper()
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	return &aiRoomHTTPAgent{
		baseURL:     strings.TrimRight(baseURL, "/"),
		siteKey:     "wa-bridge-test",
		language:    "en",
		sessionFile: sessionFile,
		client:      &http.Client{Timeout: timeout, Jar: jar},
	}
}

func postJSON(t *testing.T, client *http.Client, endpoint, body string) (string, int) {
	t.Helper()
	resp, err := client.Post(endpoint, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw), resp.StatusCode
}

func eventValues(events []map[string]any, eventName, field string) []any {
	var values []any
	for _, event := range events {
		if event["event"] == eventName {
			values = append(values, event[field])
		}
	}
	return values
}

func eventObjects(events []map[string]any, eventName string) []map[string]any {
	var matches []map[string]any
	for _, event := range events {
		if event["event"] == eventName {
			matches = append(matches, event)
		}
	}
	return matches
}

func countJSONLLines(t *testing.T, path string) int {
	t.Helper()
	file := mustOpen(t, path)
	scanner := bufio.NewScanner(file)
	lines := 0
	for scanner.Scan() {
		lines++
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return lines
}

func readJSONFile(t *testing.T, path string, dest any) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, dest); err != nil {
		t.Fatal(err)
	}
}

const aiRoomHarnessScript = `
import json
import os
import sys
import time
from types import SimpleNamespace

source = os.environ["AI_ROOM_SOURCE"]
event_log = os.environ["AI_ROOM_EVENT_LOG"]
sys.path.insert(0, source)

state = {
    "next_participant_id": 1,
    "participants_by_tg": {},
    "participants_by_id": {},
    "chat_participants": {},
    "messages": {},
    "lead_cards": {},
}

def log_event(name, **fields):
    record = {"event": name, **fields}
    with open(event_log, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=True, default=str) + "\n")

def ensure_chat(chat_id, title=None, chat_type=None):
    log_event("ensure_chat", chat_id=chat_id, title=title, chat_type=chat_type)

def get_or_create_participant(tg_user_id, username=None, display_name=None, is_bot=False, language="ru", role=None):
    existing = state["participants_by_tg"].get(tg_user_id)
    if existing:
        return existing
    pid = state["next_participant_id"]
    state["next_participant_id"] += 1
    participant = {
        "id": pid,
        "tg_user_id": tg_user_id,
        "username": username,
        "display_name": display_name,
        "kind": "agent" if is_bot else "human",
        "agent_engine": None,
        "role": role or ("agent" if is_bot else "client"),
        "language": language,
        "channels": {"text": True},
    }
    state["participants_by_tg"][tg_user_id] = participant
    state["participants_by_id"][pid] = participant
    log_event("participant", participant=participant)
    return participant

def link_participant_to_chat(participant_id, chat_id):
    state["chat_participants"].setdefault(chat_id, set()).add(participant_id)
    log_event("link_participant", participant_id=participant_id, chat_id=chat_id)
    return True

def save_message(chat_id, tg_msg_id, sender_name, msg_type, text=None, transcript=None, media_path=None, media_mime=None, is_bot=False):
    ensure_chat(chat_id)
    msg = {
        "sender_name": sender_name,
        "msg_type": msg_type,
        "text": text,
        "transcript": transcript,
        "is_bot": is_bot,
    }
    state["messages"].setdefault(chat_id, []).append(msg)
    log_event("save_message", chat_id=chat_id, sender_name=sender_name, text=text or transcript or "", is_bot=is_bot)
    return len(state["messages"][chat_id])

def load_context(chat_id, limit=50):
    rows = state["messages"].get(chat_id, [])[-limit:]
    out = []
    for row in rows:
        content = row.get("text") or row.get("transcript") or ""
        if content:
            out.append({"sender_name": row["sender_name"], "text": content, "is_bot": row["is_bot"]})
    log_event("load_context", chat_id=chat_id, count=len(out), texts=[m["text"] for m in out])
    return out

def get_chat_participants(chat_id):
    return [state["participants_by_id"][pid] for pid in state["chat_participants"].get(chat_id, set())]

def get_site_config(site_key, ttl_sec=30):
    return {
        "context_prompt": "You are the isolated AI-Room integration-test agent. Mention prior context when present.",
        "enabled_agents": ["openai"],
        "role_name": "AI Room test agent",
        "persona": "Precise, short, context-aware",
        "topic_frame": "WhatsApp voice bridge tests and telecom intake",
        "forbidden_zone": [],
        "topic_return_msg": "Return to the WhatsApp voice bridge test.",
        "off_topic_threshold": 0,
        "handover_triggers": [],
        "glossary": {},
    }

def save_lead_card(card_dict):
    state["lead_cards"][card_dict["chat_id"]] = dict(card_dict)
    log_event("save_lead_card", card=card_dict)
    return True

def get_lead_card(chat_id):
    return state["lead_cards"].get(chat_id)

import db.store as store
for name, value in {
    "ensure_chat": ensure_chat,
    "get_or_create_participant": get_or_create_participant,
    "link_participant_to_chat": link_participant_to_chat,
    "save_message": save_message,
    "load_context": load_context,
    "get_chat_participants": get_chat_participants,
    "get_site_config": get_site_config,
    "save_lead_card": save_lead_card,
    "get_lead_card": get_lead_card,
}.items():
    setattr(store, name, value)

import room.brain_agent as brain_agent
for name, value in {
    "load_context": load_context,
    "get_site_config": get_site_config,
    "get_chat_participants": get_chat_participants,
    "save_lead_card": save_lead_card,
    "get_lead_card": get_lead_card,
}.items():
    setattr(brain_agent, name, value)

class FakeCompletions:
    def create(self, model, max_completion_tokens, messages, response_format):
        user_messages = [m["content"] for m in messages if m["role"] == "user"]
        current = user_messages[-1] if user_messages else ""
        prior = user_messages[-2] if len(user_messages) > 1 else ""
        turn = len(user_messages)
        reply = f"AI Room test reply turn {turn}: current={current}"
        if prior:
            reply += f" | prior={prior}"
        reply += " | all=" + " || ".join(user_messages)
        payload = {
            "reply": reply,
            "lang": "en",
            "done": False,
            "fields": {
                "goal": "voice bridge integration",
                "note": current,
            },
        }
        log_event("model_request", model=model, messages=messages, reply=reply)
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=json.dumps(payload)))])

class FakeOpenAI:
    def __init__(self, api_key):
        self.chat = SimpleNamespace(completions=FakeCompletions())

brain_agent.OpenAI = FakeOpenAI

import web.chat_service as chat_service
for name, value in {
    "ensure_chat": ensure_chat,
    "get_or_create_participant": get_or_create_participant,
    "link_participant_to_chat": link_participant_to_chat,
    "save_message": save_message,
    "load_context": load_context,
}.items():
    setattr(chat_service, name, value)
chat_service._agents.clear()

from werkzeug.serving import make_server
server = make_server("127.0.0.1", 0, chat_service.app)
print(f"READY http://127.0.0.1:{server.server_port}", flush=True)
try:
    server.serve_forever()
finally:
    server.server_close()
`
