<p align="center">
  <a href="https://github.com/luis-kovi/evolution-go-calls">
    <img src="./public/hover-evolution.png" alt="Evolution Go Calls" />
  </a>
</p>

<h1 align="center">Evolution Go Calls</h1>

<p align="center">
  A call-focused Evolution Go fork for programmable WhatsApp calling and secure real-time audio transport.
</p>

<p align="center">
  <a href="https://github.com/luis-kovi/evolution-go-calls"><img src="https://img.shields.io/badge/fork-evolution--go-00ffa7" alt="Evolution Go fork" /></a>
  <a href="https://opensource.org/licenses/Apache-2.0"><img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License: Apache 2.0" /></a>
  <a href="https://github.com/luis-kovi/evolution-go-calls/pull/1"><img src="https://img.shields.io/badge/calls-active%20development-orange" alt="Calls: active development" /></a>
  <a href="https://github.com/evolution-foundation/evolution-go"><img src="https://img.shields.io/badge/upstream-Evolution%20Go-white" alt="Upstream Evolution Go" /></a>
</p>

<p align="center">
  <a href="#why-this-fork-exists">Why this fork exists</a> &middot;
  <a href="#call-capabilities">Call capabilities</a> &middot;
  <a href="#security-model">Security</a> &middot;
  <a href="#architecture">Architecture</a> &middot;
  <a href="#quick-start">Quick start</a> &middot;
  <a href="https://github.com/luis-kovi/evolution-go-calls/pull/1">Call PR</a>
</p>

## Rekovi call-stream build provenance

This branch starts from the call implementation proposed in source-fork PR
[`luis-kovi/evolution-go-calls#1`](https://github.com/luis-kovi/evolution-go-calls/pull/1),
whose reviewed head was `e21ce0dd7634f3047410c893b5e4d1c082034b84`. The branch then
keeps only the audio transport surface and adds lifecycle, validation, and test hardening.

`POST /call/dial` is experimental code outside the reviewed answer-call design. This
branch exposes audio calls only; video and participant-add paths are intentionally out
of scope. Browser/operator clients should use a short-lived HMAC stream URL generated
by a trusted backend, never an Evolution instance API key.


---

## About

**Evolution Go Calls** is a specialized open-source fork of [Evolution Go](https://github.com/evolution-foundation/evolution-go), a high-performance WhatsApp API written in Go and built around [whatsmeow](https://github.com/tulir/whatsmeow).

The purpose of this fork is narrow and deliberate: move WhatsApp calling beyond passive call events and rejection, toward a programmable interface that external systems can answer, stream, observe, and secure.

That makes the project useful for use cases such as:

- external voice/media pipelines
- speech processing or operator systems implemented outside this repository
- call recording and observability
- browser-based operator consoles
- automated support and contact-center workflows
- experimentation with real-time WhatsApp media transport

The original Evolution Go project remains the upstream foundation. This repository focuses on call-specific experimentation, hardening, validation, and contributions that can be useful to the wider ecosystem.

---

## Why this fork exists

Evolution Go already provides a broad WhatsApp API for messaging, media, events, persistence, queues, and integrations. Its currently documented call API in upstream focuses primarily on rejecting incoming calls.

This fork explores the next layer: **what if a WhatsApp call could be treated as a programmable real-time media session?**

The call work associated with this repository extends the model toward:

1. receiving and tracking active calls;
2. answering or terminating calls programmatically;
3. placing experimental outbound calls;
4. bridging call audio to an external WebSocket consumer;
5. accepting outbound media from that consumer;
6. isolating streams by instance and call;
7. preventing browser clients from receiving the full Evolution instance API key.

The goal is not to replace Evolution Go. The goal is to develop, test, secure, document, and eventually upstream reusable improvements around one of the most difficult real-time areas of the WhatsApp integration stack.

---

## Project status

> [!IMPORTANT]
> Call/media functionality is under active development and validation. The hardened call-stream work is tracked in [PR #1](https://github.com/luis-kovi/evolution-go-calls/pull/1). Experimental endpoints should not be treated as a stable public API until they are merged, documented, and released.

### Current direction

| Area | Status | Notes |
|---|---|---|
| Incoming call events | Available in upstream foundation | Uses Evolution Go event infrastructure |
| Call reject | Available in upstream foundation | Existing documented upstream behavior |
| Answer / hang up | Implemented in call work | Registry-backed call lifecycle |
| Real-time call media stream | Implemented in call work | Dedicated WebSocket bridge |
| Outbound dial | Experimental | Kept explicitly experimental |
| Outbound audio | Experimental | Validate with a paired device before production |
| Video / participant add | Out of scope | Not exposed by this audio-only branch |
| Per-call HMAC stream authentication | Implemented in hardened fork work | Short-lived, instance/call-scoped tokens |
| Legacy API-key stream authentication | Compatibility path | Maintained for compatibility while hardening evolves |

---

## Call capabilities

The call-focused work introduces a registry and service layer around active WhatsApp calls so they can be controlled independently from the normal message flow.

### Core call operations

The implementation work includes or explores endpoints for:

```text
POST /call/reject
POST /call/answer
POST /call/hangup
POST /call/dial              # experimental
GET  /call/stream/:callId    # WebSocket media bridge
```

Video, participant-add, and other call-control spikes from upstream PR #1 are intentionally not included in this production target.

### Why the stream matters

The WebSocket media bridge makes it possible to move call media between WhatsApp and another real-time system.

Conceptually:

```text
WhatsApp caller
      │
      │ encrypted WhatsApp call transport
      ▼
Evolution Go Calls
      │
      │ call registry + media bridge
      ▼
WebSocket stream
      │
      ├──> external speech/media pipeline
      ├──> recorder / analytics
      └──> operator application

External audio can flow back through the same bridge toward the active call.
```

This turns the call layer into infrastructure that can be integrated with systems that were never designed to understand the WhatsApp protocol directly.

---

## Security model

Real-time call streaming creates a different security profile from a conventional REST endpoint. A browser or external voice worker may need temporary access to a **single call stream**, but should not automatically receive credentials capable of controlling the entire WhatsApp instance.

The hardened call-stream work therefore introduces short-lived HMAC authentication for:

```text
GET /call/stream/:callId
```

### Design goals

- **Do not expose the full instance API key to browser operators.**
- **Scope authorization to an instance and call.**
- **Use short-lived credentials instead of long-lived stream secrets.**
- **Reject invalid authentication before the WebSocket upgrade.**
- **Prevent a token issued for one instance/call from being reused for another.**
- **Preserve a compatibility path while the hardened interface evolves.**

The current signing model binds the authorization to values equivalent to:

```text
<instance>|<callId>|<expiration>
```

and signs them with HMAC-SHA256. The hardened work uses a short connection window and validates expired, invalid, and mismatched tokens before establishing the WebSocket session.

### Why this matters

A media stream can contain sensitive voice/video data and can also be a path back into a live call. Security mistakes around stream authorization can lead to credential exposure, cross-instance access, unintended media disclosure, or control of the wrong call.

For that reason, security review is a first-class part of the project rather than an afterthought.

---

## Architecture

At a high level, the call stack is organized around four responsibilities:

```text
┌─────────────────────────────────────────────────────────────┐
│                       HTTP / WebSocket                      │
│  /call/answer  /call/hangup  /call/dial  /call/stream/... │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                       Call service                          │
│        validates requests and call lifecycle actions       │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                       Call registry                         │
│   maps active call IDs to the corresponding call objects   │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│              whatsmeow + meowcaller integration             │
│ signaling, media sources/sinks, WhatsApp call transport    │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            ▼
                         WhatsApp
```

The media bridge implements the source/sink responsibilities needed to forward media over WebSocket while keeping the call-specific code separated from the rest of the messaging stack.

---

## Validation and engineering notes

The call work has been developed with real-world behavior in mind, because successful method calls do not necessarily mean WhatsApp peers observe the expected result.

Examples discovered during live validation include:

- raw call rejection from a linked/companion session not reliably propagating as a proper decline;
- video calls not requiring an additional video-accept operation after answering when video was already part of the original offer;
- outbound video needing additional negotiation instead of assuming successful frame submission means the peer can render it;
- participant operations exhibiting timeout behavior during active calls;
- outbound media needing lifecycle gating until the remote peer accepts the call.

The hardened branch also documents validation with:

```bash
go build ./...
go vet ./...
go test ./...
```

and specific authentication/lifecycle scenarios including:

- valid HMAC vector
- expired token
- clock-skew boundary
- invalid signature
- instance mismatch
- legacy API-key compatibility
- inbound-media pre-accept gating

These checks are part of a broader goal: make call functionality reproducible and reviewable instead of relying only on manual demos.

---

## AI voice integration

One use case for the call stream is an external voice pipeline.

A typical integration can look like this:

```text
WhatsApp Call
    │
    ▼
Evolution Go Calls
    │
    ▼
WebSocket media stream
    │
    ▼
Speech-to-Text
    │
    ▼
LLM / agent logic
    │
    ▼
Text-to-Speech
    │
    ▼
WebSocket outbound audio
    │
    ▼
WhatsApp Call
```

The repository contains no AI/STT/TTS/LLM logic. It exposes only the secure transport boundary; downstream applications own processing, recording, observability, and operator UX.

---

## Upstream relationship

This repository is forked from:

**[evolution-foundation/evolution-go](https://github.com/evolution-foundation/evolution-go)**

Evolution Go is part of the Evolution Foundation ecosystem and provides the core messaging engine, REST API, event system, persistence, queue integrations, media handling, QR pairing, and other infrastructure on which this work is based.

The call implementation builds on the source-fork work in [luis-kovi/evolution-go-calls#1](https://github.com/luis-kovi/evolution-go-calls/pull/1), then adds fork-specific hardening and lifecycle fixes here.

Where improvements are generally useful and suitable for the upstream project, the preferred direction is to keep them reviewable and upstream-friendly.

---

## Base Evolution Go features

In addition to the call-focused work, the fork inherits the broader Evolution Go feature set:

- **High performance** — built with Go for minimal resource usage
- **RESTful API** — HTTP endpoints with Swagger/OpenAPI documentation
- **Real-time events** — WebSocket, Webhook, AMQP/RabbitMQ and NATS support
- **Media support** — images, videos, audio and documents with MinIO/S3 storage
- **Message storage** — optional PostgreSQL persistence
- **QR code pairing** — built-in QR code generation for device linking
- **License management** — registration, activation and heartbeat support
- **Docker ready** — container-oriented deployment workflow

---

## Quick Start

### Clone this fork

```bash
git clone https://github.com/luis-kovi/evolution-go-calls.git
cd evolution-go-calls
```

### Docker

```bash
make docker-build
make docker-run
```

### Local development

```bash
make setup
cp .env.example .env
make dev
```

> Run `make help` to see the commands exposed by the repository. See [COMMANDS.md](./COMMANDS.md) for the upstream development workflows when available.

---

## Configuration

Create a `.env` file based on `.env.example` and configure the services you actually use.

Example baseline:

```env
# Server
SERVER_PORT=8080
CLIENT_NAME=evolution

# Security
GLOBAL_API_KEY=your-secure-api-key-here

# Database
POSTGRES_AUTH_DB=postgresql://postgres:password@localhost:5432/evogo_auth?sslmode=disable
POSTGRES_USERS_DB=postgresql://postgres:password@localhost:5432/evogo_users?sslmode=disable
DATABASE_SAVE_MESSAGES=false

# Logging
WADEBUG=DEBUG
LOGTYPE=console

# Optional integrations
# AMQP_URL=amqp://guest:guest@localhost:5672/
# NATS_URL=nats://localhost:4222
# WEBHOOK_URL=https://your-webhook-url.com/webhook
# MINIO_ENABLED=true
# MINIO_ENDPOINT=localhost:9000
# MINIO_ACCESS_KEY=minioadmin
# MINIO_SECRET_KEY=minioadmin
```

| Variable | Description | Default |
|---|---|---|
| `SERVER_PORT` | Server port | `8080` |
| `CLIENT_NAME` | Client identifier | `evolution` |
| `GLOBAL_API_KEY` | Global API authentication key | **Required** |
| `DATABASE_SAVE_MESSAGES` | Enable message storage | `false` |
| `WADEBUG` | WhatsApp debug level | `INFO` |

> [!CAUTION]
> Never embed `GLOBAL_API_KEY` or a full instance API key in browser-side code. For call streaming, prefer short-lived scoped authorization mechanisms such as the HMAC design being developed in this fork.

---

## License activation

The inherited Evolution Go runtime includes its existing licensing/activation flow. On first run, follow the upstream project requirements for activation and runtime configuration.

Typical flow:

1. start the server;
2. open the manager interface;
3. provide the configured API URL and `GLOBAL_API_KEY`;
4. complete the required activation flow;
5. confirm the API is operational before testing call features.

Refer to the upstream documentation for the current licensing behavior.

---

## API documentation

Swagger UI is available when generated/enabled by the application, typically at:

```text
http://localhost:8080/swagger/index.html
```

### Base endpoints

Examples inherited from Evolution Go include:

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/instance/create` | Create WhatsApp instance |
| `GET` | `/instance/{name}/qrcode` | Get QR code for pairing |
| `POST` | `/message/sendText` | Send text message |
| `POST` | `/message/sendMedia` | Send media message |
| `GET` | `/instance/{name}/status` | Get instance status |
| `DELETE` | `/instance/{name}` | Delete instance |

### Call-focused endpoints

The active call work adds or explores:

| Method | Endpoint | Status | Purpose |
|---|---|---|---|
| `POST` | `/call/reject` | Upstream/base | Reject incoming call |
| `POST` | `/call/answer` | Call work | Answer tracked incoming call |
| `POST` | `/call/hangup` | Call work | Terminate tracked call |
| `POST` | `/call/dial` | Experimental | Place outbound call |
| `GET` | `/call/stream/:callId` | Call work | Bidirectional call media WebSocket |

Check the active branch/PR and generated Swagger before relying on an endpoint in production.

---

## Project structure

The project retains the Evolution Go structure and adds call-focused components around the service/route layers.

```text
evolution-go-calls/
├── cmd/evolution-go/        # Application entry point
├── pkg/
│   ├── call/                # Call registry / call media building blocks
│   ├── core/                # License management & middleware
│   ├── instance/            # Instance management
│   ├── message/             # Message handling
│   ├── sendMessage/         # Message sending
│   ├── routes/              # HTTP routes
│   ├── middleware/          # Auth & validation middleware
│   ├── config/              # Configuration
│   ├── events/              # Event producers (AMQP, NATS, Webhook, WS)
│   └── storage/             # Media storage (MinIO)
├── docs/                    # Swagger / project documentation
├── Dockerfile
├── Makefile
└── VERSION
```

> Exact paths can vary by branch while the call implementation is being reviewed and integrated.

---

## Tech stack

| Component | Technology |
|---|---|
| Language | Go 1.24+ |
| HTTP framework | Gin |
| WhatsApp protocol | [whatsmeow](https://github.com/tulir/whatsmeow) |
| Call media integration | [meowcaller](https://github.com/purpshell/meowcaller) in call-development work |
| Database | PostgreSQL |
| ORM | GORM |
| Message queues | RabbitMQ, NATS |
| Object storage | MinIO/S3 |
| API documentation | Swagger/OpenAPI |
| Containers | Docker |
| Stream authorization | HMAC-SHA256 design in hardened call work |

---

## Roadmap

The near-term direction for this fork is to make WhatsApp calling safer, easier to integrate, and easier to review.

- [ ] Merge and stabilize hardened call-stream authentication
- [ ] Expand automated tests around call lifecycle and stream authorization
- [ ] Document the WebSocket media protocol with complete message examples
- [ ] Add reference AI voice integration examples
- [ ] Improve observability for call state transitions and media failures
- [ ] Separate stable call operations from experimental controls
- [ ] Add reproducible integration-test guidance for real WhatsApp calls
- [ ] Review security boundaries for browser/operator use cases
- [ ] Upstream generally useful fixes where appropriate

---

## Contributing

Contributions are welcome, especially around:

- call lifecycle correctness
- real-time audio/media handling
- WebSocket resilience
- authentication and authorization
- automated testing
- security review
- protocol documentation
- AI voice integration examples
- observability and debugging

Before opening a large PR, consider starting with an issue or discussion that explains the behavior being changed and how it was validated.

For upstream-generic contributions unrelated to the call fork, also consider contributing directly to [Evolution Go](https://github.com/evolution-foundation/evolution-go).

---

## Security

Call streaming is security-sensitive. Please do **not** publish credentials, API keys, signing secrets, real call payloads, or private media in issues or pull requests.

For vulnerabilities inherited from or affecting Evolution Go broadly, follow the upstream project's [SECURITY.md](./SECURITY.md) and private vulnerability reporting guidance.

For fork-specific call-stream findings, prefer GitHub private vulnerability reporting when available and provide enough detail to reproduce the issue without exposing production secrets or user data.

Security-sensitive areas include:

- WebSocket authentication
- HMAC signing and expiration validation
- instance/call authorization boundaries
- credential handling in browser clients
- call registry isolation
- media-stream lifecycle cleanup
- cross-instance access
- logging of call metadata and media

---

## Responsible use

This software interacts with WhatsApp accounts, call metadata, and potentially real-time voice/video media. Deployers are responsible for complying with applicable laws, privacy requirements, consent obligations, platform terms, and organizational security policies.

Use test accounts and controlled environments when validating experimental call functionality.

---

## Documentation and references

| Resource | Link |
|---|---|
| This fork | [github.com/luis-kovi/evolution-go-calls](https://github.com/luis-kovi/evolution-go-calls) |
| Hardened call-stream PR | [luis-kovi/evolution-go-calls#1](https://github.com/luis-kovi/evolution-go-calls/pull/1) |
| Upstream Evolution Go | [github.com/evolution-foundation/evolution-go](https://github.com/evolution-foundation/evolution-go) |
| Source-fork call PR | [luis-kovi/evolution-go-calls#1](https://github.com/luis-kovi/evolution-go-calls/pull/1) |
| Evolution documentation | [docs.evolutionfoundation.com.br](https://docs.evolutionfoundation.com.br) |
| Evolution community | [evolutionfoundation.com.br/community](https://evolutionfoundation.com.br/community) |
| Changelog | [CHANGELOG.md](./CHANGELOG.md) |
| Contributing | [CONTRIBUTING.md](./CONTRIBUTING.md) |
| Security | [SECURITY.md](./SECURITY.md) |

---

## Acknowledgments

This repository exists because of the work of the broader open-source community around Evolution Go and WhatsApp interoperability.

Special thanks to:

- [Evolution Foundation](https://github.com/evolution-foundation) — upstream Evolution Go project and ecosystem
- [Tulir Asokan](https://github.com/tulir) and contributors to [whatsmeow](https://github.com/tulir/whatsmeow) — WhatsApp protocol foundation
- [purpshell/meowcaller](https://github.com/purpshell/meowcaller) contributors — call/media building blocks used by the call-development work
- contributors to the upstream call implementation and reviewers validating real call behavior

Fork maintenance and call-stream hardening in this repository are coordinated through [luis-kovi/evolution-go-calls](https://github.com/luis-kovi/evolution-go-calls).

---

## License

The repository inherits the upstream project's licensing files and requirements. Evolution Go is distributed under the Apache License 2.0 together with the additional notices/brand conditions documented by the upstream repository.

See [LICENSE](./LICENSE), [NOTICE](./NOTICE), and [TRADEMARKS.md](./TRADEMARKS.md) for the exact terms applicable to the codebase.

---

## Trademarks and upstream attribution

"Evolution Foundation", "Evolution", and "Evolution Go" are trademarks of Evolution Foundation. This repository is a public fork and should not be interpreted as replacing the upstream project or as implying endorsement beyond the relationship visible in the GitHub fork history.

Original project:

**[Evolution Foundation / Evolution Go](https://github.com/evolution-foundation/evolution-go)**

---

<p align="center">
  Built on Evolution Go · focused on secure, programmable WhatsApp calling
</p>
