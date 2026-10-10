// Command ai-room-voice-bridge is an offline prototype adapter for the
// Evolution Go WhatsApp call stream and AI-Room.
//
// It does not answer calls, dial WhatsApp, configure webhooks, or connect to a
// live call stream. Inputs are captured artifacts: a 16 kHz mono PCM16 WAV or
// JSONL messages using Evolution Go's /call/stream wire format.
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	sampleRate   = 16000
	frameSamples = 960
	frameBytes   = frameSamples * 2
)

type config struct {
	inWAV             string
	inJSONL           string
	outDir            string
	sttProvider       string
	fixedTranscript   string
	whisperBin        string
	whisperModel      string
	whisperLanguage   string
	agentProvider     string
	mockReply         string
	aiRoomURL         string
	aiRoomSiteKey     string
	aiRoomLanguage    string
	aiRoomSessionFile string
	allowNetwork      bool
	allowAIHost       string
	ttsProvider       string
	fixedTTSWAV       string
	sayBin            string
	ffmpegBin         string
	voice             string
	maxReplyChars     int
	sttTimeout        time.Duration
	agentTimeout      time.Duration
	ttsTimeout        time.Duration
	logText           bool
}

type streamMessage struct {
	Event      string         `json:"event"`
	CallID     string         `json:"callId,omitempty"`
	SampleRate int            `json:"sampleRate,omitempty"`
	Track      string         `json:"track,omitempty"`
	Payload    string         `json:"payload,omitempty"`
	Reason     string         `json:"reason,omitempty"`
	Phase      string         `json:"phase,omitempty"`
	Direction  string         `json:"direction,omitempty"`
	Stats      map[string]any `json:"stats,omitempty"`
}

type eventLogger struct {
	mu  sync.Mutex
	enc *json.Encoder
}

type commandResult struct {
	stdout   string
	stderr   string
	duration time.Duration
}

type audioStats struct {
	SampleRate     int   `json:"sample_rate"`
	Channels       int   `json:"channels"`
	BitsPerSample  int   `json:"bits_per_sample"`
	Samples        int   `json:"samples"`
	Frames960      int   `json:"frames_960"`
	DurationMS     int64 `json:"duration_ms"`
	NonZeroSamples int   `json:"non_zero_samples"`
	Peak           int   `json:"peak"`
	FrameAligned   bool  `json:"frame_aligned"`
}

type runSummary struct {
	InputPath         string            `json:"input_path"`
	InputFormat       string            `json:"input_format"`
	InputSHA256       string            `json:"input_sha256"`
	OutputDir         string            `json:"output_dir"`
	InboundWAVPath    string            `json:"inbound_wav_path"`
	TranscriptPath    string            `json:"transcript_path"`
	ReplyPath         string            `json:"reply_path"`
	ResponseWAVPath   string            `json:"response_wav_path"`
	OutboundJSONLPath string            `json:"outbound_jsonl_path"`
	LogPath           string            `json:"log_path"`
	Providers         map[string]string `json:"providers"`
	TimingsMS         map[string]int64  `json:"timings_ms"`
	InputAudio        audioStats        `json:"input_audio"`
	ResponseAudio     audioStats        `json:"response_audio"`
	TranscriptChars   int               `json:"transcript_chars"`
	ReplyChars        int               `json:"reply_chars"`
}

type transcriber interface {
	Transcribe(context.Context, string) (string, error)
	Name() string
}

type textAgent interface {
	Reply(context.Context, ConversationTurn) (string, error)
	Name() string
}

type synthesizer interface {
	Synthesize(context.Context, string, string) (audioStats, error)
	Name() string
}

type ConversationTurn struct {
	SessionID string
	Role      string
	Text      string
	CallID    string
	Direction string
	Language  string
}

func main() {
	var c config
	flag.StringVar(&c.inWAV, "in-wav", "", "offline inbound WAV artifact: 16 kHz mono PCM16 preferred")
	flag.StringVar(&c.inJSONL, "in-jsonl", "", "offline JSONL with Evolution /call/stream media messages")
	flag.StringVar(&c.outDir, "out-dir", "", "private output directory; default creates a temp directory")
	flag.StringVar(&c.sttProvider, "stt", env("WA_BRIDGE_STT", "fixed"), "fixed or whisper")
	flag.StringVar(&c.fixedTranscript, "fixed-transcript", env("WA_BRIDGE_FIXED_TRANSCRIPT", "Olá, estou a testar a ponte offline."), "test transcript for -stt=fixed")
	flag.StringVar(&c.whisperBin, "whisper-bin", env("WHISPER_CLI", "whisper-cli"), "whisper.cpp CLI binary")
	flag.StringVar(&c.whisperModel, "whisper-model", env("WHISPER_MODEL", ""), "local whisper.cpp model path")
	flag.StringVar(&c.whisperLanguage, "language", env("WHISPER_LANGUAGE", "auto"), "Whisper/AI-Room language, e.g. auto, pt, en")
	flag.StringVar(&c.agentProvider, "agent", env("WA_BRIDGE_AGENT", "mock"), "mock or ai-room-http")
	flag.StringVar(&c.mockReply, "mock-reply", env("WA_BRIDGE_MOCK_REPLY", "Recebido. Esta é uma resposta offline de teste."), "deterministic reply for -agent=mock")
	flag.StringVar(&c.aiRoomURL, "ai-room-url", env("AI_ROOM_URL", ""), "AI-Room chat service base URL; future/local use only")
	flag.StringVar(&c.aiRoomSiteKey, "ai-room-site-key", env("AI_ROOM_SITE_KEY", "one-card"), "AI-Room site_key for /chat/<site_key>")
	flag.StringVar(&c.aiRoomLanguage, "ai-room-language", env("AI_ROOM_LANGUAGE", "pt"), "AI-Room web session language")
	flag.StringVar(&c.aiRoomSessionFile, "ai-room-session-file", "", "optional cookie session file, stored 0600")
	flag.BoolVar(&c.allowNetwork, "allow-network", false, "allow AI-Room HTTP adapter to make network calls")
	flag.StringVar(&c.allowAIHost, "allow-ai-host", "", "explicit extra AI-Room host allowlist entry; .198 remains blocked")
	flag.StringVar(&c.ttsProvider, "tts", env("WA_BRIDGE_TTS", "tone"), "tone, say, or wav")
	flag.StringVar(&c.fixedTTSWAV, "tts-wav", "", "existing response WAV for -tts=wav")
	flag.StringVar(&c.sayBin, "say-bin", env("SAY_BIN", "say"), "macOS say binary")
	flag.StringVar(&c.ffmpegBin, "ffmpeg-bin", env("FFMPEG_BIN", "ffmpeg"), "ffmpeg binary")
	flag.StringVar(&c.voice, "voice", env("SAY_VOICE", "auto"), "macOS say voice, or auto")
	flag.IntVar(&c.maxReplyChars, "max-reply-chars", 280, "hard cap for spoken reply")
	flag.DurationVar(&c.sttTimeout, "stt-timeout", 90*time.Second, "STT timeout")
	flag.DurationVar(&c.agentTimeout, "agent-timeout", 30*time.Second, "agent timeout")
	flag.DurationVar(&c.ttsTimeout, "tts-timeout", 30*time.Second, "TTS timeout")
	flag.BoolVar(&c.logText, "log-text", false, "include transcript/reply text in JSONL logs; off by default")
	flag.Parse()

	if err := run(context.Background(), c); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, c config) error {
	if (c.inWAV == "") == (c.inJSONL == "") {
		return errors.New("provide exactly one of -in-wav or -in-jsonl")
	}
	if c.maxReplyChars <= 0 {
		return errors.New("-max-reply-chars must be positive")
	}
	outDir, err := ensureOutDir(c.outDir)
	if err != nil {
		return err
	}
	logPath := filepath.Join(outDir, "bridge.jsonl")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	defer logFile.Close()
	logger := &eventLogger{enc: json.NewEncoder(logFile)}

	inputPath, inputFormat := c.inWAV, "wav"
	if c.inJSONL != "" {
		inputPath, inputFormat = c.inJSONL, "jsonl"
	}
	inputSHA, err := sha256File(inputPath)
	if err != nil {
		return err
	}
	logger.write("bridge_started", map[string]any{
		"input":      inputPath,
		"format":     inputFormat,
		"sha256":     inputSHA,
		"outDir":     outDir,
		"stt":        c.sttProvider,
		"agent":      c.agentProvider,
		"tts":        c.ttsProvider,
		"logText":    c.logText,
		"liveBridge": false,
	})

	timings := map[string]int64{}
	stepStart := time.Now()
	samples, inputStats, streamMeta, err := loadInboundSamples(c)
	if err != nil {
		return err
	}
	timings["input_load"] = time.Since(stepStart).Milliseconds()
	if inputStats.NonZeroSamples == 0 {
		return errors.New("input audio has no non-zero samples; refusing to produce bot audio")
	}

	inboundWAVPath := filepath.Join(outDir, "inbound-normalized-16k-mono-pcm16.wav")
	samples = padSamples(samples, frameSamples)
	if err := writePCM16MonoWAV(inboundWAVPath, samples); err != nil {
		return err
	}
	inputStats = computeStats(samples)
	logger.write("input_loaded", map[string]any{
		"path":       inboundWAVPath,
		"callId":     streamMeta.CallID,
		"direction":  streamMeta.Direction,
		"samples":    inputStats.Samples,
		"frames960":  inputStats.Frames960,
		"nonZero":    inputStats.NonZeroSamples,
		"durationMs": inputStats.DurationMS,
	})

	stt, err := buildTranscriber(c)
	if err != nil {
		return err
	}
	stepStart = time.Now()
	transcript, err := stt.Transcribe(ctx, inboundWAVPath)
	if err != nil {
		return err
	}
	timings["stt"] = time.Since(stepStart).Milliseconds()
	transcript = cleanSpeechText(transcript)
	if transcript == "" {
		return errors.New("STT produced an empty transcript; refusing to produce bot audio")
	}
	transcriptPath := filepath.Join(outDir, "transcript.txt")
	if err := writePrivateText(transcriptPath, transcript+"\n"); err != nil {
		return err
	}
	logger.write("stt_completed", textFields(c.logText, transcript, map[string]any{
		"provider":   stt.Name(),
		"path":       transcriptPath,
		"chars":      utf8.RuneCountInString(transcript),
		"durationMs": timings["stt"],
	}))

	agent, err := buildAgent(c)
	if err != nil {
		return err
	}
	stepStart = time.Now()
	agentCtx, cancelAgent := context.WithTimeout(ctx, c.agentTimeout)
	reply, err := agent.Reply(agentCtx, ConversationTurn{
		SessionID: stableSessionID(streamMeta.CallID, inputSHA),
		Role:      "caller",
		Text:      transcript,
		CallID:    streamMeta.CallID,
		Direction: coalesce(streamMeta.Direction, "incoming"),
		Language:  c.aiRoomLanguage,
	})
	cancelAgent()
	if err != nil {
		return err
	}
	timings["agent"] = time.Since(stepStart).Milliseconds()
	reply = truncateRunes(cleanSpeechText(reply), c.maxReplyChars)
	if reply == "" {
		return errors.New("agent produced an empty reply; refusing to produce bot audio")
	}
	replyPath := filepath.Join(outDir, "reply.txt")
	if err := writePrivateText(replyPath, reply+"\n"); err != nil {
		return err
	}
	logger.write("agent_completed", textFields(c.logText, reply, map[string]any{
		"provider":   agent.Name(),
		"path":       replyPath,
		"chars":      utf8.RuneCountInString(reply),
		"durationMs": timings["agent"],
	}))

	tts, err := buildSynthesizer(c, outDir)
	if err != nil {
		return err
	}
	responseWAVPath := filepath.Join(outDir, "response-16k-mono-pcm16-960frames.wav")
	stepStart = time.Now()
	responseStats, err := tts.Synthesize(ctx, reply, responseWAVPath)
	if err != nil {
		return err
	}
	timings["tts"] = time.Since(stepStart).Milliseconds()
	if !responseStats.FrameAligned {
		return errors.New("TTS output is not aligned to 960-sample frames")
	}
	outboundJSONLPath := filepath.Join(outDir, "outbound-media.jsonl")
	responseSamples, _, err := readPCM16MonoWAV(responseWAVPath)
	if err != nil {
		return err
	}
	if err := writeOutboundJSONL(outboundJSONLPath, streamMeta.CallID, responseSamples); err != nil {
		return err
	}
	logger.write("tts_completed", map[string]any{
		"provider":   tts.Name(),
		"path":       responseWAVPath,
		"jsonl":      outboundJSONLPath,
		"samples":    responseStats.Samples,
		"frames960":  responseStats.Frames960,
		"durationMs": timings["tts"],
	})

	summary := runSummary{
		InputPath:         inputPath,
		InputFormat:       inputFormat,
		InputSHA256:       inputSHA,
		OutputDir:         outDir,
		InboundWAVPath:    inboundWAVPath,
		TranscriptPath:    transcriptPath,
		ReplyPath:         replyPath,
		ResponseWAVPath:   responseWAVPath,
		OutboundJSONLPath: outboundJSONLPath,
		LogPath:           logPath,
		Providers: map[string]string{
			"stt":   stt.Name(),
			"agent": agent.Name(),
			"tts":   tts.Name(),
		},
		TimingsMS:       timings,
		InputAudio:      inputStats,
		ResponseAudio:   responseStats,
		TranscriptChars: utf8.RuneCountInString(transcript),
		ReplyChars:      utf8.RuneCountInString(reply),
	}
	summaryPath := filepath.Join(outDir, "run.json")
	if err := writePrivateJSON(summaryPath, summary); err != nil {
		return err
	}
	logger.write("bridge_completed", map[string]any{"summary": summaryPath})
	printSummary(summary)
	return nil
}

type streamMeta struct {
	CallID    string
	Direction string
}

func loadInboundSamples(c config) ([]int16, audioStats, streamMeta, error) {
	if c.inWAV != "" {
		samples, stats, err := readPCM16MonoWAV(c.inWAV)
		return samples, stats, streamMeta{Direction: "incoming"}, err
	}
	return readStreamJSONL(c.inJSONL)
}

func readStreamJSONL(path string) ([]int16, audioStats, streamMeta, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, audioStats{}, streamMeta{}, err
	}
	defer file.Close()
	var samples []int16
	var meta streamMeta
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var msg streamMessage
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			return nil, audioStats{}, streamMeta{}, fmt.Errorf("invalid JSONL line: %w", err)
		}
		if msg.CallID != "" && meta.CallID == "" {
			meta.CallID = msg.CallID
		}
		if msg.Direction != "" && meta.Direction == "" {
			meta.Direction = msg.Direction
		}
		if msg.Event != "media" || msg.Track != "inbound" {
			continue
		}
		if msg.SampleRate != 0 && msg.SampleRate != sampleRate {
			return nil, audioStats{}, streamMeta{}, fmt.Errorf("unsupported sampleRate %d; want %d", msg.SampleRate, sampleRate)
		}
		raw, err := base64.StdEncoding.DecodeString(msg.Payload)
		if err != nil {
			return nil, audioStats{}, streamMeta{}, fmt.Errorf("invalid media payload: %w", err)
		}
		if len(raw) != frameBytes {
			return nil, audioStats{}, streamMeta{}, fmt.Errorf("invalid frame bytes %d; want %d", len(raw), frameBytes)
		}
		for offset := 0; offset < len(raw); offset += 2 {
			samples = append(samples, int16(binary.LittleEndian.Uint16(raw[offset:])))
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, audioStats{}, streamMeta{}, err
	}
	if len(samples) == 0 {
		return nil, audioStats{}, streamMeta{}, errors.New("JSONL contained no inbound media frames")
	}
	stats := computeStats(samples)
	stats.Channels = 1
	stats.BitsPerSample = 16
	return samples, stats, meta, nil
}

func buildTranscriber(c config) (transcriber, error) {
	switch c.sttProvider {
	case "fixed":
		return fixedTranscriber{text: c.fixedTranscript}, nil
	case "whisper":
		if strings.TrimSpace(c.whisperModel) == "" {
			return nil, errors.New("-whisper-model is required for -stt=whisper")
		}
		return whisperTranscriber{bin: c.whisperBin, model: c.whisperModel, language: c.whisperLanguage, timeout: c.sttTimeout}, nil
	default:
		return nil, fmt.Errorf("unsupported -stt %q", c.sttProvider)
	}
}

func buildAgent(c config) (textAgent, error) {
	switch c.agentProvider {
	case "mock":
		return mockAgent{reply: c.mockReply}, nil
	case "ai-room-http":
		if err := validateAIHTTPConfig(c); err != nil {
			return nil, err
		}
		jar, _ := cookiejar.New(nil)
		return &aiRoomHTTPAgent{
			baseURL:     strings.TrimRight(c.aiRoomURL, "/"),
			siteKey:     c.aiRoomSiteKey,
			language:    c.aiRoomLanguage,
			sessionFile: c.aiRoomSessionFile,
			client:      &http.Client{Timeout: c.agentTimeout, Jar: jar},
		}, nil
	default:
		return nil, fmt.Errorf("unsupported -agent %q", c.agentProvider)
	}
}

func buildSynthesizer(c config, outDir string) (synthesizer, error) {
	switch c.ttsProvider {
	case "tone":
		return toneSynthesizer{}, nil
	case "say":
		return saySynthesizer{sayBin: c.sayBin, ffmpegBin: c.ffmpegBin, voice: resolveVoice(c.voice, c.aiRoomLanguage), timeout: c.ttsTimeout, workDir: outDir}, nil
	case "wav":
		if c.fixedTTSWAV == "" {
			return nil, errors.New("-tts-wav is required for -tts=wav")
		}
		return wavSynthesizer{path: c.fixedTTSWAV}, nil
	default:
		return nil, fmt.Errorf("unsupported -tts %q", c.ttsProvider)
	}
}

type fixedTranscriber struct{ text string }

func (f fixedTranscriber) Transcribe(context.Context, string) (string, error) { return f.text, nil }
func (f fixedTranscriber) Name() string                                       { return "fixed" }

type whisperTranscriber struct {
	bin, model, language string
	timeout              time.Duration
}

func (w whisperTranscriber) Transcribe(ctx context.Context, wavPath string) (string, error) {
	outBase := strings.TrimSuffix(wavPath, filepath.Ext(wavPath)) + ".whisper"
	args := []string{"-m", w.model, "-f", wavPath, "-l", w.language, "-otxt", "-of", outBase, "-np"}
	result, err := runCommand(ctx, w.timeout, w.bin, args, "")
	if err != nil {
		return "", fmt.Errorf("whisper failed: %w\n%s", err, result.stderr)
	}
	raw, err := os.ReadFile(outBase + ".txt")
	if err != nil {
		if strings.TrimSpace(result.stdout) != "" {
			return result.stdout, nil
		}
		return "", err
	}
	return string(raw), nil
}

func (w whisperTranscriber) Name() string { return "whisper.cpp:" + filepath.Base(w.model) }

type mockAgent struct{ reply string }

func (m mockAgent) Reply(context.Context, ConversationTurn) (string, error) { return m.reply, nil }
func (m mockAgent) Name() string                                            { return "mock" }

type aiRoomHTTPAgent struct {
	baseURL, siteKey, language, sessionFile string
	client                                  *http.Client
}

func (a *aiRoomHTTPAgent) Reply(ctx context.Context, turn ConversationTurn) (string, error) {
	if a.sessionFile != "" {
		if err := a.loadSession(); err != nil {
			return "", err
		}
	}
	initURL := fmt.Sprintf("%s/chat/%s?lang=%s", a.baseURL, url.PathEscape(a.siteKey), url.QueryEscape(a.language))
	if _, err := a.do(ctx, http.MethodGet, initURL, nil); err != nil {
		return "", fmt.Errorf("AI-Room session init failed: %w", err)
	}
	body, _ := json.Marshal(map[string]string{"message": turn.Text})
	raw, err := a.do(ctx, http.MethodPost, a.baseURL+"/api/chat/send", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("AI-Room send failed: %w", err)
	}
	var parsed struct {
		OK    bool   `json:"ok"`
		Reply string `json:"reply"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return "", fmt.Errorf("AI-Room invalid JSON: %w", err)
	}
	if !parsed.OK {
		return "", fmt.Errorf("AI-Room returned ok=false: %s", parsed.Error)
	}
	if a.sessionFile != "" {
		if err := a.saveSession(); err != nil {
			return "", err
		}
	}
	return parsed.Reply, nil
}

func (a *aiRoomHTTPAgent) Name() string { return "ai-room-http:" + a.siteKey }

func (a *aiRoomHTTPAgent) do(ctx context.Context, method, endpoint string, body io.Reader) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return nil, err
	}
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(raw)))
	}
	return raw, nil
}

func (a *aiRoomHTTPAgent) loadSession() error {
	raw, err := os.ReadFile(a.sessionFile)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	var cookies []*http.Cookie
	if err := json.Unmarshal(raw, &cookies); err != nil {
		return err
	}
	u, err := url.Parse(a.baseURL)
	if err != nil {
		return err
	}
	a.client.Jar.SetCookies(u, cookies)
	return nil
}

func (a *aiRoomHTTPAgent) saveSession() error {
	u, err := url.Parse(a.baseURL)
	if err != nil {
		return err
	}
	raw, err := json.MarshalIndent(a.client.Jar.Cookies(u), "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	return os.WriteFile(a.sessionFile, raw, 0600)
}

func validateAIHTTPConfig(c config) error {
	if !c.allowNetwork {
		return errors.New("-agent=ai-room-http requires explicit -allow-network; use -agent=mock for offline tests")
	}
	if c.aiRoomURL == "" {
		return errors.New("-ai-room-url is required for -agent=ai-room-http")
	}
	u, err := url.Parse(c.aiRoomURL)
	if err != nil {
		return err
	}
	host := u.Hostname()
	if host == "" || u.Scheme == "" {
		return errors.New("-ai-room-url must include scheme and host")
	}
	if isForbiddenHost(host) {
		return fmt.Errorf("AI-Room host %s is explicitly blocked for this offline prototype", host)
	}
	if isLoopbackHost(host) {
		return nil
	}
	if c.allowAIHost != "" && strings.EqualFold(host, c.allowAIHost) {
		return nil
	}
	return fmt.Errorf("AI-Room host %s is not loopback; pass -allow-ai-host=%s only for an approved non-production lab host", host, host)
}

func isForbiddenHost(host string) bool {
	host = strings.Trim(host, "[]")
	return host == "81.17.140.198"
}

func isLoopbackHost(host string) bool {
	host = strings.Trim(host, "[]")
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

type toneSynthesizer struct{}

func (toneSynthesizer) Synthesize(_ context.Context, _ string, output string) (audioStats, error) {
	samples := make([]int16, frameSamples*8)
	for i := range samples {
		v := math.Sin(2 * math.Pi * 440 * float64(i) / sampleRate)
		samples[i] = int16(v * 9000)
	}
	if err := writePCM16MonoWAV(output, samples); err != nil {
		return audioStats{}, err
	}
	return computeStats(samples), nil
}

func (toneSynthesizer) Name() string { return "tone" }

type wavSynthesizer struct{ path string }

func (w wavSynthesizer) Synthesize(_ context.Context, _ string, output string) (audioStats, error) {
	samples, _, err := readPCM16MonoWAV(w.path)
	if err != nil {
		return audioStats{}, err
	}
	samples = padSamples(samples, frameSamples)
	if err := writePCM16MonoWAV(output, samples); err != nil {
		return audioStats{}, err
	}
	return computeStats(samples), nil
}

func (w wavSynthesizer) Name() string { return "wav:" + filepath.Base(w.path) }

type saySynthesizer struct {
	sayBin, ffmpegBin, voice, workDir string
	timeout                           time.Duration
}

func (s saySynthesizer) Synthesize(ctx context.Context, text, output string) (audioStats, error) {
	aiffPath := filepath.Join(s.workDir, "tts.aiff")
	if result, err := runCommand(ctx, s.timeout, s.sayBin, []string{"-v", s.voice, "-o", aiffPath, text}, ""); err != nil {
		return audioStats{}, fmt.Errorf("say failed: %w\n%s", err, result.stderr)
	}
	rawWAV := filepath.Join(s.workDir, "tts-raw-16k-mono-pcm16.wav")
	if err := convertToPCM16(ctx, s.ffmpegBin, s.timeout, aiffPath, rawWAV); err != nil {
		return audioStats{}, err
	}
	samples, _, err := readPCM16MonoWAV(rawWAV)
	if err != nil {
		return audioStats{}, err
	}
	samples = padSamples(samples, frameSamples)
	if err := writePCM16MonoWAV(output, samples); err != nil {
		return audioStats{}, err
	}
	return computeStats(samples), nil
}

func (s saySynthesizer) Name() string { return "say:" + s.voice }

func ensureOutDir(path string) (string, error) {
	if path == "" {
		dir, err := os.MkdirTemp("", "ai-room-voice-bridge-*")
		if err != nil {
			return "", err
		}
		if err := os.Chmod(dir, 0700); err != nil {
			return "", err
		}
		return dir, nil
	}
	if err := os.MkdirAll(path, 0700); err != nil {
		return "", err
	}
	if err := os.Chmod(path, 0700); err != nil {
		return "", err
	}
	return path, nil
}

func convertToPCM16(ctx context.Context, ffmpegBin string, timeout time.Duration, input, output string) error {
	args := []string{"-hide_banner", "-nostdin", "-y", "-i", input, "-vn", "-ac", "1", "-ar", fmt.Sprint(sampleRate), "-sample_fmt", "s16", output}
	result, err := runCommand(ctx, timeout, ffmpegBin, args, "")
	if err != nil {
		return fmt.Errorf("ffmpeg failed: %w\n%s", err, result.stderr)
	}
	return os.Chmod(output, 0600)
}

func runCommand(parent context.Context, timeout time.Duration, bin string, args []string, stdin string) (commandResult, error) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = append(os.Environ(), "NO_COLOR=1", "TERM=dumb")
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	result := commandResult{stdout: stdout.String(), stderr: stderr.String(), duration: time.Since(start)}
	if ctx.Err() == context.DeadlineExceeded {
		return result, fmt.Errorf("%s timed out after %s", bin, timeout)
	}
	return result, err
}

func readPCM16MonoWAV(path string) ([]int16, audioStats, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, audioStats{}, err
	}
	if len(raw) < 44 || string(raw[0:4]) != "RIFF" || string(raw[8:12]) != "WAVE" {
		return nil, audioStats{}, fmt.Errorf("%s is not a RIFF/WAVE file", path)
	}
	var channels, rate, bits uint16
	var data []byte
	for offset := 12; offset+8 <= len(raw); {
		id := string(raw[offset : offset+4])
		size := int(binary.LittleEndian.Uint32(raw[offset+4 : offset+8]))
		chunkStart := offset + 8
		chunkEnd := chunkStart + size
		if chunkEnd > len(raw) {
			return nil, audioStats{}, fmt.Errorf("%s has truncated %s chunk", path, id)
		}
		if id == "fmt " {
			if size < 16 {
				return nil, audioStats{}, fmt.Errorf("%s has invalid fmt chunk", path)
			}
			format := binary.LittleEndian.Uint16(raw[chunkStart:])
			channels = binary.LittleEndian.Uint16(raw[chunkStart+2:])
			rate32 := binary.LittleEndian.Uint32(raw[chunkStart+4:])
			bits = binary.LittleEndian.Uint16(raw[chunkStart+14:])
			if format != 1 || channels != 1 || rate32 != sampleRate || bits != 16 {
				return nil, audioStats{}, fmt.Errorf("%s must be PCM16LE mono %d Hz; got format=%d channels=%d rate=%d bits=%d", path, sampleRate, format, channels, rate32, bits)
			}
			rate = uint16(rate32)
		} else if id == "data" {
			data = raw[chunkStart:chunkEnd]
		}
		offset = chunkEnd
		if offset%2 == 1 {
			offset++
		}
	}
	if data == nil {
		return nil, audioStats{}, fmt.Errorf("%s missing data chunk", path)
	}
	if len(data)%2 != 0 {
		return nil, audioStats{}, fmt.Errorf("%s has odd PCM data length", path)
	}
	samples := make([]int16, len(data)/2)
	for i := range samples {
		samples[i] = int16(binary.LittleEndian.Uint16(data[i*2:]))
	}
	stats := computeStats(samples)
	stats.SampleRate = int(rate)
	stats.Channels = int(channels)
	stats.BitsPerSample = int(bits)
	return samples, stats, nil
}

func writePCM16MonoWAV(path string, samples []int16) error {
	var buf bytes.Buffer
	dataSize := uint32(len(samples) * 2)
	if _, err := buf.WriteString("RIFF"); err != nil {
		return err
	}
	_ = binary.Write(&buf, binary.LittleEndian, uint32(36)+dataSize)
	_, _ = buf.WriteString("WAVEfmt ")
	_ = binary.Write(&buf, binary.LittleEndian, uint32(16))
	_ = binary.Write(&buf, binary.LittleEndian, uint16(1))
	_ = binary.Write(&buf, binary.LittleEndian, uint16(1))
	_ = binary.Write(&buf, binary.LittleEndian, uint32(sampleRate))
	_ = binary.Write(&buf, binary.LittleEndian, uint32(sampleRate*2))
	_ = binary.Write(&buf, binary.LittleEndian, uint16(2))
	_ = binary.Write(&buf, binary.LittleEndian, uint16(16))
	_, _ = buf.WriteString("data")
	_ = binary.Write(&buf, binary.LittleEndian, dataSize)
	for _, sample := range samples {
		_ = binary.Write(&buf, binary.LittleEndian, sample)
	}
	return os.WriteFile(path, buf.Bytes(), 0600)
}

func writeOutboundJSONL(path, callID string, samples []int16) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	enc := json.NewEncoder(file)
	samples = padSamples(samples, frameSamples)
	for offset := 0; offset < len(samples); offset += frameSamples {
		payload := make([]byte, frameBytes)
		for i := 0; i < frameSamples; i++ {
			binary.LittleEndian.PutUint16(payload[i*2:], uint16(samples[offset+i]))
		}
		msg := streamMessage{
			Event:      "media",
			CallID:     callID,
			SampleRate: sampleRate,
			Track:      "outbound",
			Payload:    base64.StdEncoding.EncodeToString(payload),
		}
		if err := enc.Encode(msg); err != nil {
			return err
		}
	}
	return nil
}

func computeStats(samples []int16) audioStats {
	stats := audioStats{
		SampleRate:    sampleRate,
		Channels:      1,
		BitsPerSample: 16,
		Samples:       len(samples),
		Frames960:     len(samples) / frameSamples,
		DurationMS:    int64(len(samples)) * 1000 / sampleRate,
		FrameAligned:  len(samples)%frameSamples == 0,
	}
	for _, sample := range samples {
		if sample != 0 {
			stats.NonZeroSamples++
		}
		peak := int(sample)
		if peak < 0 {
			peak = -peak
		}
		if peak > stats.Peak {
			stats.Peak = peak
		}
	}
	return stats
}

func padSamples(samples []int16, frameSize int) []int16 {
	if frameSize <= 0 || len(samples)%frameSize == 0 {
		return samples
	}
	padded := make([]int16, len(samples)+(frameSize-len(samples)%frameSize))
	copy(padded, samples)
	return padded
}

func sha256File(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func cleanSpeechText(value string) string {
	value = stripANSI(value)
	value = strings.ReplaceAll(value, "\r", "\n")
	lines := strings.Split(value, "\n")
	var kept []string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "output_") || strings.HasPrefix(line, "whisper_") {
			continue
		}
		kept = append(kept, line)
	}
	return strings.Join(kept, " ")
}

var ansiRE = regexp.MustCompile(`\x1b(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])`)

func stripANSI(value string) string {
	value = ansiRE.ReplaceAllString(value, "")
	value = strings.Map(func(r rune) rune {
		if r == '\n' || r == '\t' || r == '\r' || r >= 0x20 {
			return r
		}
		return -1
	}, value)
	return strings.TrimSpace(value)
}

func truncateRunes(value string, max int) string {
	runes := []rune(value)
	if len(runes) <= max {
		return value
	}
	return strings.TrimSpace(string(runes[:max]))
}

func resolveVoice(requested, language string) string {
	if requested != "" && requested != "auto" {
		return requested
	}
	switch strings.ToLower(language) {
	case "pt":
		return "Joana"
	case "en":
		return "Samantha"
	default:
		return "Milena"
	}
}

func textFields(include bool, text string, fields map[string]any) map[string]any {
	if include {
		fields["text"] = text
	}
	return fields
}

func coalesce(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func stableSessionID(callID, sha string) string {
	if callID != "" {
		return "wa-call:" + callID
	}
	if len(sha) >= 16 {
		return "wa-offline:" + sha[:16]
	}
	return "wa-offline"
}

func writePrivateText(path, text string) error {
	return os.WriteFile(path, []byte(text), 0600)
}

func writePrivateJSON(path string, value any) error {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	return os.WriteFile(path, raw, 0600)
}

func (l *eventLogger) write(event string, fields map[string]any) {
	record := map[string]any{"ts": time.Now().UTC().Format(time.RFC3339Nano), "event": event}
	for key, value := range fields {
		record[key] = value
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	_ = l.enc.Encode(record)
}

func printSummary(s runSummary) {
	raw, _ := json.MarshalIndent(s, "", "  ")
	fmt.Println(string(raw))
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
