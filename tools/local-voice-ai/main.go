// Command local-voice-ai runs an offline voice AI sidecar pipeline over an
// existing recorded WAV: Whisper STT -> local LLM -> local TTS.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
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
)

type config struct {
	input           string
	outDir          string
	whisperBin      string
	whisperModel    string
	whisperLanguage string
	llmProvider     string
	ollamaBin       string
	ollamaModel     string
	llamaBin        string
	llamaModel      string
	systemPrompt    string
	sayBin          string
	ffmpegBin       string
	voice           string
	maxReplyChars   int
	sttTimeout      time.Duration
	llmTimeout      time.Duration
	ttsTimeout      time.Duration
	logText         bool
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

type runSummary struct {
	InputPath       string            `json:"input_path"`
	InputSHA256     string            `json:"input_sha256"`
	OutputDir       string            `json:"output_dir"`
	STTInputPath    string            `json:"stt_input_path"`
	TranscriptPath  string            `json:"transcript_path"`
	ReplyPath       string            `json:"reply_path"`
	ResponseWAVPath string            `json:"response_wav_path"`
	LogPath         string            `json:"log_path"`
	Providers       map[string]string `json:"providers"`
	TimingsMS       map[string]int64  `json:"timings_ms"`
	Audio           audioStats        `json:"audio"`
	TranscriptChars int               `json:"transcript_chars"`
	ReplyChars      int               `json:"reply_chars"`
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

func main() {
	var c config
	flag.StringVar(&c.input, "in", "", "existing inbound WAV or audio file to process")
	flag.StringVar(&c.outDir, "out-dir", "", "private output directory; default creates a temp directory")
	flag.StringVar(&c.whisperBin, "whisper-bin", env("WHISPER_CLI", "whisper-cli"), "whisper.cpp CLI binary")
	flag.StringVar(&c.whisperModel, "whisper-model", env("WHISPER_MODEL", ""), "local whisper.cpp model path")
	flag.StringVar(&c.whisperLanguage, "language", env("WHISPER_LANGUAGE", "auto"), "Whisper language, e.g. auto, pt, en")
	flag.StringVar(&c.llmProvider, "llm-provider", env("LOCAL_VOICE_LLM_PROVIDER", "auto"), "auto, ollama, llama-cli, or template")
	flag.StringVar(&c.ollamaBin, "ollama-bin", env("OLLAMA_BIN", "ollama"), "Ollama binary")
	flag.StringVar(&c.ollamaModel, "ollama-model", env("OLLAMA_MODEL", "gemma3:4b"), "installed local Ollama model")
	flag.StringVar(&c.llamaBin, "llama-bin", env("LLAMA_CLI", "llama-cli"), "llama.cpp CLI binary")
	flag.StringVar(&c.llamaModel, "llama-model", env("LLAMA_MODEL", ""), "local GGUF path for llama-cli")
	flag.StringVar(&c.sayBin, "say-bin", env("SAY_BIN", "say"), "macOS say binary")
	flag.StringVar(&c.ffmpegBin, "ffmpeg-bin", env("FFMPEG_BIN", "ffmpeg"), "ffmpeg binary")
	flag.StringVar(&c.voice, "voice", env("SAY_VOICE", "auto"), "macOS say voice, or auto for script-based selection")
	flag.StringVar(&c.systemPrompt, "system-prompt", defaultSystemPrompt(), "local call assistant instruction")
	flag.IntVar(&c.maxReplyChars, "max-reply-chars", 280, "hard cap for spoken reply text")
	flag.DurationVar(&c.sttTimeout, "stt-timeout", 90*time.Second, "STT timeout")
	flag.DurationVar(&c.llmTimeout, "llm-timeout", 60*time.Second, "LLM timeout")
	flag.DurationVar(&c.ttsTimeout, "tts-timeout", 30*time.Second, "TTS timeout")
	flag.BoolVar(&c.logText, "log-text", false, "include transcript/reply text in JSONL logs; off by default")
	flag.Parse()

	if err := run(context.Background(), c); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, c config) error {
	if strings.TrimSpace(c.input) == "" {
		return errors.New("-in is required")
	}
	if strings.TrimSpace(c.whisperModel) == "" {
		return errors.New("-whisper-model is required; use an existing local ggml*.bin model")
	}
	if c.maxReplyChars <= 0 {
		return errors.New("-max-reply-chars must be positive")
	}
	outDir, err := ensureOutDir(c.outDir)
	if err != nil {
		return err
	}
	logPath := filepath.Join(outDir, "pipeline.jsonl")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	defer logFile.Close()
	l := &eventLogger{enc: json.NewEncoder(logFile)}

	inputSHA, err := sha256File(c.input)
	if err != nil {
		return err
	}
	l.write("pipeline_started", map[string]any{
		"input":        c.input,
		"inputSHA256":  inputSHA,
		"outDir":       outDir,
		"logText":      c.logText,
		"whisperModel": filepath.Base(c.whisperModel),
		"llmProvider":  c.llmProvider,
	})

	timings := map[string]int64{}
	sttInput := filepath.Join(outDir, "stt-input-16k-mono-pcm16.wav")
	stepStart := time.Now()
	if err := convertToPCM16(ctx, c.ffmpegBin, c.ttsTimeout, c.input, sttInput); err != nil {
		return err
	}
	timings["input_convert"] = time.Since(stepStart).Milliseconds()
	l.write("input_prepared", map[string]any{"path": sttInput, "durationMs": timings["input_convert"]})

	stepStart = time.Now()
	transcript, err := transcribe(ctx, c, outDir)
	if err != nil {
		return err
	}
	timings["stt"] = time.Since(stepStart).Milliseconds()
	transcript = cleanSpeechText(transcript)
	if transcript == "" {
		return errors.New("STT produced an empty transcript")
	}
	transcriptPath := filepath.Join(outDir, "transcript.txt")
	if err := writePrivateText(transcriptPath, transcript+"\n"); err != nil {
		return err
	}
	l.write("stt_completed", textFields(c.logText, transcript, map[string]any{
		"path":       transcriptPath,
		"chars":      utf8.RuneCountInString(transcript),
		"durationMs": timings["stt"],
	}))

	provider, err := resolveLLMProvider(ctx, c)
	if err != nil {
		return err
	}
	stepStart = time.Now()
	reply, err := generateReply(ctx, c, provider, transcript)
	if err != nil {
		return err
	}
	timings["llm"] = time.Since(stepStart).Milliseconds()
	reply = truncateRunes(cleanSpeechText(reply), c.maxReplyChars)
	if reply == "" {
		return errors.New("LLM produced an empty reply")
	}
	replyPath := filepath.Join(outDir, "reply.txt")
	if err := writePrivateText(replyPath, reply+"\n"); err != nil {
		return err
	}
	l.write("llm_completed", textFields(c.logText, reply, map[string]any{
		"provider":   provider,
		"model":      llmModelName(c, provider),
		"path":       replyPath,
		"chars":      utf8.RuneCountInString(reply),
		"durationMs": timings["llm"],
	}))

	responsePath := filepath.Join(outDir, "response-16k-mono-pcm16-960frames.wav")
	stepStart = time.Now()
	ttsVoice := resolveVoice(c.voice, reply)
	c.voice = ttsVoice
	stats, err := synthesize(ctx, c, outDir, reply, responsePath)
	if err != nil {
		return err
	}
	timings["tts"] = time.Since(stepStart).Milliseconds()
	l.write("tts_completed", map[string]any{
		"path":         responsePath,
		"durationMs":   timings["tts"],
		"samples":      stats.Samples,
		"frames960":    stats.Frames960,
		"frameAligned": stats.FrameAligned,
		"peak":         stats.Peak,
	})

	summary := runSummary{
		InputPath:       c.input,
		InputSHA256:     inputSHA,
		OutputDir:       outDir,
		STTInputPath:    sttInput,
		TranscriptPath:  transcriptPath,
		ReplyPath:       replyPath,
		ResponseWAVPath: responsePath,
		LogPath:         logPath,
		Providers: map[string]string{
			"stt": "whisper.cpp:" + filepath.Base(c.whisperModel),
			"llm": provider + ":" + llmModelName(c, provider),
			"tts": "say:" + ttsVoice,
		},
		TimingsMS:       timings,
		Audio:           stats,
		TranscriptChars: utf8.RuneCountInString(transcript),
		ReplyChars:      utf8.RuneCountInString(reply),
	}
	summaryPath := filepath.Join(outDir, "run.json")
	if err := writePrivateJSON(summaryPath, summary); err != nil {
		return err
	}
	l.write("pipeline_completed", map[string]any{"summary": summaryPath, "response": responsePath})
	printSummary(summary)
	return nil
}

func ensureOutDir(path string) (string, error) {
	if path == "" {
		dir, err := os.MkdirTemp("", "local-voice-ai-*")
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

func transcribe(ctx context.Context, c config, outDir string) (string, error) {
	base := filepath.Join(outDir, "whisper")
	args := []string{"-m", c.whisperModel, "-f", filepath.Join(outDir, "stt-input-16k-mono-pcm16.wav"), "-l", c.whisperLanguage, "-otxt", "-of", base, "-np"}
	result, err := runCommand(ctx, c.sttTimeout, c.whisperBin, args, "")
	if err != nil {
		return "", fmt.Errorf("whisper failed: %w\n%s", err, result.stderr)
	}
	raw, err := os.ReadFile(base + ".txt")
	if err != nil {
		if strings.TrimSpace(result.stdout) != "" {
			return result.stdout, nil
		}
		return "", err
	}
	return string(raw), nil
}

func resolveLLMProvider(ctx context.Context, c config) (string, error) {
	switch c.llmProvider {
	case "ollama", "llama-cli", "template":
		return c.llmProvider, nil
	case "auto":
		if _, err := exec.LookPath(c.ollamaBin); err == nil && c.ollamaModel != "" {
			return "ollama", nil
		}
		if _, err := exec.LookPath(c.llamaBin); err == nil && c.llamaModel != "" {
			return "llama-cli", nil
		}
		return "", errors.New("no local LLM provider found; install/use Ollama model or pass -llama-model /path/model.gguf")
	default:
		return "", fmt.Errorf("unsupported -llm-provider %q", c.llmProvider)
	}
}

func generateReply(ctx context.Context, c config, provider, transcript string) (string, error) {
	prompt := buildPrompt(c.systemPrompt, transcript)
	switch provider {
	case "ollama":
		result, err := runCommand(ctx, c.llmTimeout, c.ollamaBin, []string{"run", c.ollamaModel, prompt}, "")
		if err != nil {
			return "", fmt.Errorf("ollama failed: %w\n%s", err, result.stderr)
		}
		return stripANSI(result.stdout), nil
	case "llama-cli":
		if c.llamaModel == "" {
			return "", errors.New("-llama-model is required for llama-cli")
		}
		args := []string{"-m", c.llamaModel, "-p", prompt, "-n", "96", "--temp", "0.4", "--no-display-prompt"}
		result, err := runCommand(ctx, c.llmTimeout, c.llamaBin, args, "")
		if err != nil {
			return "", fmt.Errorf("llama-cli failed: %w\n%s", err, result.stderr)
		}
		return stripANSI(result.stdout), nil
	case "template":
		return "Entendi. " + transcript, nil
	default:
		return "", fmt.Errorf("unsupported LLM provider %q", provider)
	}
}

func synthesize(ctx context.Context, c config, outDir, text, responsePath string) (audioStats, error) {
	aiffPath := filepath.Join(outDir, "tts.aiff")
	if result, err := runCommand(ctx, c.ttsTimeout, c.sayBin, []string{"-v", c.voice, "-o", aiffPath, text}, ""); err != nil {
		return audioStats{}, fmt.Errorf("say failed: %w\n%s", err, result.stderr)
	}
	rawWAV := filepath.Join(outDir, "tts-raw-16k-mono-pcm16.wav")
	if err := convertToPCM16(ctx, c.ffmpegBin, c.ttsTimeout, aiffPath, rawWAV); err != nil {
		return audioStats{}, err
	}
	samples, stats, err := readPCM16MonoWAV(rawWAV)
	if err != nil {
		return audioStats{}, err
	}
	samples = padSamples(samples, frameSamples)
	if err := writePCM16MonoWAV(responsePath, samples); err != nil {
		return audioStats{}, err
	}
	stats = computeStats(samples)
	return stats, nil
}

func resolveVoice(requested, text string) string {
	if requested != "" && requested != "auto" {
		return requested
	}
	for _, r := range text {
		if r >= '\u0400' && r <= '\u04FF' {
			return "Milena"
		}
	}
	return "Joana"
}

func convertToPCM16(ctx context.Context, ffmpegBin string, timeout time.Duration, input, output string) error {
	args := []string{"-hide_banner", "-nostdin", "-y", "-i", input, "-vn", "-ac", "1", "-ar", fmt.Sprint(sampleRate), "-sample_fmt", "s16", output}
	result, err := runCommand(ctx, timeout, ffmpegBin, args, "")
	if err != nil {
		return fmt.Errorf("ffmpeg failed: %w\n%s", err, result.stderr)
	}
	if err := os.Chmod(output, 0600); err != nil {
		return err
	}
	return nil
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

func buildPrompt(systemPrompt, transcript string) string {
	return strings.TrimSpace(systemPrompt) + "\n\nUser said:\n" + strings.TrimSpace(transcript) + "\n\nReply for TTS:"
}

func defaultSystemPrompt() string {
	return "You are a local offline WhatsApp voice assistant for GoIPpro. Disclose you are an AI when relevant. Reply in the user's language, in one concise spoken sentence. Do not mention tools, transcripts, logs, or implementation details."
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

func textFields(include bool, text string, fields map[string]any) map[string]any {
	if include {
		fields["text"] = text
	}
	return fields
}

func llmModelName(c config, provider string) string {
	switch provider {
	case "ollama":
		return c.ollamaModel
	case "llama-cli":
		return filepath.Base(c.llamaModel)
	case "template":
		return "deterministic-test-template"
	default:
		return ""
	}
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
		switch id {
		case "fmt ":
			if size < 16 {
				return nil, audioStats{}, fmt.Errorf("%s has short fmt chunk", path)
			}
			format := binary.LittleEndian.Uint16(raw[chunkStart : chunkStart+2])
			if format != 1 {
				return nil, audioStats{}, fmt.Errorf("%s must be PCM format, got %d", path, format)
			}
			channels = binary.LittleEndian.Uint16(raw[chunkStart+2 : chunkStart+4])
			rate = uint16(binary.LittleEndian.Uint32(raw[chunkStart+4 : chunkStart+8]))
			bits = binary.LittleEndian.Uint16(raw[chunkStart+14 : chunkStart+16])
		case "data":
			data = raw[chunkStart:chunkEnd]
		}
		offset = chunkEnd
		if offset%2 == 1 {
			offset++
		}
	}
	if channels != 1 || rate != sampleRate || bits != 16 {
		return nil, audioStats{}, fmt.Errorf("%s must be 16 kHz mono PCM16, got channels=%d rate=%d bits=%d", path, channels, rate, bits)
	}
	if len(data)%2 != 0 {
		return nil, audioStats{}, fmt.Errorf("%s data chunk has odd byte count", path)
	}
	samples := make([]int16, len(data)/2)
	for i := range samples {
		samples[i] = int16(binary.LittleEndian.Uint16(data[i*2:]))
	}
	return samples, computeStats(samples), nil
}

func padSamples(samples []int16, frame int) []int16 {
	if frame <= 0 || len(samples)%frame == 0 {
		return samples
	}
	padded := make([]int16, len(samples)+(frame-len(samples)%frame))
	copy(padded, samples)
	return padded
}

func writePCM16MonoWAV(path string, samples []int16) error {
	dataBytes := len(samples) * 2
	raw := make([]byte, 44+dataBytes)
	copy(raw[0:4], "RIFF")
	binary.LittleEndian.PutUint32(raw[4:8], uint32(36+dataBytes))
	copy(raw[8:12], "WAVE")
	copy(raw[12:16], "fmt ")
	binary.LittleEndian.PutUint32(raw[16:20], 16)
	binary.LittleEndian.PutUint16(raw[20:22], 1)
	binary.LittleEndian.PutUint16(raw[22:24], 1)
	binary.LittleEndian.PutUint32(raw[24:28], sampleRate)
	binary.LittleEndian.PutUint32(raw[28:32], sampleRate*2)
	binary.LittleEndian.PutUint16(raw[32:34], 2)
	binary.LittleEndian.PutUint16(raw[34:36], 16)
	copy(raw[36:40], "data")
	binary.LittleEndian.PutUint32(raw[40:44], uint32(dataBytes))
	for i, sample := range samples {
		binary.LittleEndian.PutUint16(raw[44+i*2:], uint16(sample))
	}
	return os.WriteFile(path, raw, 0600)
}

func computeStats(samples []int16) audioStats {
	var nonZero, peak int
	for _, sample := range samples {
		abs := int(math.Abs(float64(sample)))
		if abs > 8 {
			nonZero++
		}
		if abs > peak {
			peak = abs
		}
	}
	frames := 0
	if len(samples) > 0 {
		frames = len(samples) / frameSamples
	}
	return audioStats{
		SampleRate:     sampleRate,
		Channels:       1,
		BitsPerSample:  16,
		Samples:        len(samples),
		Frames960:      frames,
		DurationMS:     int64(len(samples)) * 1000 / sampleRate,
		NonZeroSamples: nonZero,
		Peak:           peak,
		FrameAligned:   len(samples)%frameSamples == 0,
	}
}

func printSummary(summary runSummary) {
	raw, _ := json.MarshalIndent(summary, "", "  ")
	fmt.Println(string(raw))
}

func env(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
