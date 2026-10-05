# WhatsApp Voice Calls — handoff для установки и live-проверки

## 1. Назначение

Этот репозиторий — call-focused fork Evolution Go для реальных 1:1 WhatsApp audio calls:

- принять входящий WhatsApp-звонок через API;
- поднять WebSocket media bridge для аудио потока;
- записать inbound audio в WAV;
- отправить deterministic marker обратно в звонок;
- экспериментально инициировать исходящий WhatsApp-звонок;
- проверить, что через bridge прошел ненулевой двусторонний audio media.

Это не UI-продукт и не готовый contact-center. Это рабочий backend/harness для установки, pairing и live E2E в реальном WhatsApp audio call.

## 2. Текущий проверенный статус

Дата проверки: 2026-10-05, локальная машина Валеры.

Проверено на ветке `codex/whatsapp-voice-calls`, remote `origin`:

```text
https://github.com/goippro-main/evolution-go-calls.git
```

Проверенный implementation commit до добавления этого handoff-документа:

```text
54843bab479fece17e318651d4b3889c4f2985c8
```

Локальная ветка перед документационным handoff была чистой и синхронизированной с `origin/codex/whatsapp-voice-calls`:

```text
HEAD == origin/codex/whatsapp-voice-calls == 54843bab479fece17e318651d4b3889c4f2985c8
ahead/behind: 0/0
```

Пройденные проверки:

```bash
go test ./...
go test -race ./pkg/call/... ./pkg/whatsmeow/...
go vet ./...
go build ./...
```

Результат всех команд: PASS, exit code 0.

Live WhatsApp media еще не подтверждался на реальном аккаунте в этом handoff. Честный статус: код, harness и deterministic tests готовы; финальный PASS требует pairing аккаунта A и реального звонка.

## 3. Что реализовано

API call-control:

```text
POST /call/reject
POST /call/answer
POST /call/hangup
POST /call/dial              # experimental outbound
GET  /call/stream/:callId    # WebSocket media bridge
```

Media bridge:

- mono audio;
- 16 kHz;
- PCM16LE payload в WebSocket messages;
- inbound track из WhatsApp в клиент;
- outbound playout из клиента обратно в звонок;
- counters/diagnostics для inbound/outbound frames;
- `bidirectionalMedia=true` только при ненулевом audio в обе стороны.

Security:

- preferred auth для `/call/stream/:callId` — short-lived HMAC token;
- HMAC bind: `instance|callId|expiration`;
- signing key: `CALL_STREAM_SIGNING_KEY`;
- legacy `?apikey=` path оставлен только для совместимости;
- browser/operator client не должен получать полный instance API key.

E2E harness:

- inbound mode: ждет реальный CallOffer, отвечает, открывает stream, пишет WAV, отправляет marker, завершает звонок;
- outbound mode: вызывает `/call/dial`, ждет peer accept, открывает stream, пишет WAV, отправляет marker, завершает звонок;
- пишет `lifecycle.jsonl`;
- падает, если audio evidence недостаточный.

## 4. Репозиторий, ветка, commit

GitHub repo:

```text
https://github.com/goippro-main/evolution-go-calls
```

Рабочая ветка:

```text
codex/whatsapp-voice-calls
```

Не merge в `main` без отдельного решения. Эта ветка должна оставаться отдельной handoff/installation веткой.

Implementation commit, на котором прогнаны проверки:

```text
54843bab479fece17e318651d4b3889c4f2985c8
```

После коммита этого документа final remote HEAD будет новым documentation commit поверх `54843bab...`; его SHA нужно брать из `git rev-parse origin/codex/whatsapp-voice-calls`.

## 5. Архитектура

Поток входящего звонка:

```text
WhatsApp caller
  -> whatsmeow/meowcaller
  -> incoming call callback
  -> CallRegistry.Store(instanceID, call)
  -> webhook CALL event
  -> POST /call/answer
  -> GET /call/stream/:callId
  -> WebSocket media bridge
  -> WAV recorder / external media worker
```

Поток исходящего звонка:

```text
POST /call/dial
  -> meowcaller.Client.Call(number)
  -> CallRegistry.StoreOutgoing(instanceID, call)
  -> peer_accept
  -> GET /call/stream/:callId
  -> WebSocket media bridge
```

Call registry scoped by instance: stream/answer/hangup не должны получить call из чужого instance.

Bridge lifecycle:

- inbound media не отдается до active/peer accept;
- outbound frames до readiness игнорируются;
- под backpressure новые outbound frames dropаются, чтобы не блокировать teardown/control;
- при закрытии stream пишет diagnostics и stop.

## 6. Ключевые пути

Основной сервер:

```text
cmd/evolution-go/main.go
pkg/routes/routes.go
pkg/config/env/env.go
```

Call API:

```text
pkg/call/handler/call_handler.go
pkg/call/service/call_service.go
pkg/call/registry/call_registry.go
```

WebSocket audio bridge:

```text
pkg/call/stream/handler.go
pkg/call/stream/bridge.go
pkg/call/stream/codec.go
```

WhatsApp/meowcaller integration:

```text
pkg/whatsmeow/service/whatsmeow.go
```

Live E2E harness:

```text
tools/whatsapp-call-e2e/main.go
tools/whatsapp-call-e2e/main_test.go
tools/whatsapp-call-e2e/run-inbound.sh
tools/whatsapp-call-e2e/run-outbound.sh
tools/whatsapp-call-e2e/README.md
```

Operator docs:

```text
docs/whatsapp-call-e2e.md
WHATSAPP_VOICE_CALLS_HANDOFF.md
```

License/brand:

```text
LICENSE
NOTICE
TRADEMARKS.md
README.md
```

## 7. Prerequisites

Для локальной установки:

- Go 1.25+; проверено на Go 1.26.0 darwin/arm64;
- PostgreSQL с двумя DB: auth DB и users DB;
- reachable HTTP API port, например `4000`;
- dedicated WhatsApp test account A для pairing;
- отдельный обычный WhatsApp account/номер B для звонка;
- `CALL_STREAM_SIGNING_KEY`, одинаковый у сервиса и harness;
- webhook URL, достижимый из процесса Evolution Go.

Docker daemon не нужен для unit/build checks, но для service runtime нужен PostgreSQL. AMQP, NATS, MinIO и proxy не нужны для минимального call E2E.

## 8. Local setup

Клонировать именно handoff-ветку:

```bash
git clone --branch codex/whatsapp-voice-calls https://github.com/goippro-main/evolution-go-calls.git
cd evolution-go-calls
```

Создать локальное окружение. Не коммитить `.env`, реальные API keys, license codes, WhatsApp credentials, pairing codes, QR, cookies или tokens.

Минимальные env для call E2E:

```bash
export SERVER_PORT=4000
export POSTGRES_AUTH_DB='postgresql://<user>:<password>@<host>:<port>/evogo_auth?sslmode=disable'
export POSTGRES_USERS_DB='postgresql://<user>:<password>@<host>:<port>/evogo_users?sslmode=disable'
export DATABASE_SAVE_MESSAGES=false
export GLOBAL_API_KEY='<local-admin-key>'
export CLIENT_NAME=evolution
export CONNECT_ON_STARTUP=false
export CALL_STREAM_SIGNING_KEY='<local-stream-signing-key>'
```

Локально проверить код:

```bash
go test ./...
go test -race ./pkg/call/... ./pkg/whatsmeow/...
go vet ./...
go build ./...
```

Запуск сервиса:

```bash
go run ./cmd/evolution-go
```

API base:

```bash
export API='http://127.0.0.1:4000'
```

## 9. License note

Проект содержит `LICENSE`, `NOTICE`, `TRADEMARKS.md`.

Коротко:

- код основан на Evolution Go;
- license file заявляет Apache License 2.0 plus additional Evolution Go commercial/notification conditions;
- frontend/brand assets имеют отдельные trademark restrictions;
- при production installation нельзя удалять обязательные notice/logo/copyright элементы, если используется frontend/brand;
- для закрытого/коммерческого использования нужно проверить условия `LICENSE` и при необходимости связаться с Evolution Foundation.

Этот handoff не заменяет юридическую проверку лицензии.

## 10. Instance creation и pairing

Создать instance только если он еще не создан:

```bash
export EVOLUTION_INSTANCE='wa-call-e2e'
export EVOLUTION_INSTANCE_API_KEY='<dedicated-instance-token>'

curl -X POST "$API/instance/create" \
  -H "apikey: $GLOBAL_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"name":"wa-call-e2e","token":"<dedicated-instance-token>"}'
```

Подписать instance на CALL events:

```bash
curl -X POST "$API/instance/connect" \
  -H "apikey: $EVOLUTION_INSTANCE_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"subscribe":["CALL"]}'
```

QR flow:

```bash
curl "$API/instance/qr" \
  -H "apikey: $EVOLUTION_INSTANCE_API_KEY"
```

Pairing-code flow:

```bash
curl -X POST "$API/instance/pair" \
  -H "apikey: $EVOLUTION_INSTANCE_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"phone":"<international-number-without-plus>"}'
```

Проверить статус:

```bash
curl "$API/instance/status" \
  -H "apikey: $EVOLUTION_INSTANCE_API_KEY"
```

Важно: QR/pairing code меняют внешний WhatsApp account. Это ручное действие владельца аккаунта. Не выполнять без явного подтверждения.

## 11. Inbound live test command

Условия:

- Evolution Go уже запущен;
- account A уже paired;
- webhook URL достижим из Evolution Go;
- account B готов позвонить на account A в WhatsApp.

Если Evolution Go запущен локально на той же машине:

```bash
export EVOLUTION_API_URL="$API"
export EVOLUTION_INSTANCE='wa-call-e2e'
export EVOLUTION_INSTANCE_API_KEY='<dedicated-instance-token>'
export CALL_STREAM_SIGNING_KEY='<local-stream-signing-key>'

go run ./tools/whatsapp-call-e2e \
  -mode inbound \
  -api "$EVOLUTION_API_URL" \
  -instance "$EVOLUTION_INSTANCE" \
  -apikey "$EVOLUTION_INSTANCE_API_KEY" \
  -signing-key "$CALL_STREAM_SIGNING_KEY" \
  -webhook-url http://127.0.0.1:8090/webhook \
  -out-dir ./e2e-artifacts/inbound
```

Если Evolution Go в Docker/container и должен достучаться до host:

```bash
go run ./tools/whatsapp-call-e2e \
  -mode inbound \
  -api "$EVOLUTION_API_URL" \
  -instance "$EVOLUTION_INSTANCE" \
  -apikey "$EVOLUTION_INSTANCE_API_KEY" \
  -signing-key "$CALL_STREAM_SIGNING_KEY" \
  -webhook-url http://host.docker.internal:8090/webhook \
  -webhook-listen 0.0.0.0:8090 \
  -out-dir ./e2e-artifacts/inbound
```

После старта harness человек с account B звонит в WhatsApp на account A.

## 12. Outbound live test command

Условия:

- Evolution Go уже запущен;
- account A уже paired;
- account B — обычный WhatsApp номер, готов принять звонок.

```bash
export EVOLUTION_API_URL="$API"
export EVOLUTION_INSTANCE='wa-call-e2e'
export EVOLUTION_INSTANCE_API_KEY='<dedicated-instance-token>'
export CALL_STREAM_SIGNING_KEY='<local-stream-signing-key>'
export WHATSAPP_TEST_NUMBER='<ordinary-whatsapp-number-with-country-code-no-plus>'

go run ./tools/whatsapp-call-e2e \
  -mode outbound \
  -number "$WHATSAPP_TEST_NUMBER" \
  -api "$EVOLUTION_API_URL" \
  -instance "$EVOLUTION_INSTANCE" \
  -apikey "$EVOLUTION_INSTANCE_API_KEY" \
  -signing-key "$CALL_STREAM_SIGNING_KEY" \
  -webhook-url http://127.0.0.1:8090/webhook \
  -out-dir ./e2e-artifacts/outbound
```

Для контейнера:

```bash
go run ./tools/whatsapp-call-e2e \
  -mode outbound \
  -number "$WHATSAPP_TEST_NUMBER" \
  -api "$EVOLUTION_API_URL" \
  -instance "$EVOLUTION_INSTANCE" \
  -apikey "$EVOLUTION_INSTANCE_API_KEY" \
  -signing-key "$CALL_STREAM_SIGNING_KEY" \
  -webhook-url http://host.docker.internal:8090/webhook \
  -webhook-listen 0.0.0.0:8090 \
  -out-dir ./e2e-artifacts/outbound
```

Account B должен принять звонок и подтвердить, что слышит marker 440/880/660 Hz.

## 13. Artifacts

Harness пишет артефакты в `-out-dir`:

```text
inbound-<callId>.wav
lifecycle.jsonl
```

WAV:

- received inbound track;
- 16 kHz mono PCM16LE;
- должен содержать ненулевое аудио.

`lifecycle.jsonl`:

- UTC lifecycle events;
- call direction/state;
- per-frame timestamps;
- recorder counters;
- server diagnostics;
- WAV path;
- errors/fail reason.

Не коммитить `e2e-artifacts/`, WAV, lifecycle logs с реальными identifiers, tokens или номерами.

## 14. PASS criteria

Нельзя объявлять PASS по одному из этих признаков:

- HTTP 200;
- ringing;
- connected state;
- keepalive;
- WebSocket `start`;
- пустой WAV;
- silent-only frames.

PASS требует все пункты:

- service запущен на ветке `codex/whatsapp-voice-calls`;
- instance A paired и `GET /instance/status` показывает рабочее состояние;
- `subscribe:["CALL"]` применен;
- harness завершился exit code 0;
- `lifecycle.jsonl` содержит выбранный real callId;
- recorder: `inboundFrames > 0`;
- recorder: `nonZeroFrames > 0`;
- WAV существует и содержит ненулевое audio;
- server diagnostics: `outboundFrames > 0`;
- server diagnostics: `outboundNonZeroFrames > 0`;
- server diagnostics: `bidirectionalMedia == true`;
- человек на удаленном account B подтвердил, что слышал marker 440/880/660 Hz;
- для inbound: call инициирован с account B на paired account A;
- для outbound: account B принял звонок от paired account A.

Если любой счетчик отсутствует или равен нулю, это FAIL, даже если звонок визуально был connected.

## 15. Known limitations

- Реализован только 1:1 audio call.
- Group calls не реализованы.
- Multi-participant recording не реализован.
- Раздельные tracks для двух удаленных участников не реализованы.
- Video не входит в контракт этой ветки.
- UI/operator console не входит в контракт.
- Claude/Moshi/speech pipeline не входит в контракт.
- `/call/dial` experimental и должен считаться нестабильным API.
- Live media нельзя честно подтвердить без paired WhatsApp account A и обычного account B.
- `CALL_STREAM_SIGNING_KEY` должен быть одинаковым у сервиса и harness.
- Legacy stream auth через `?apikey=` существует для совместимости, но не должен использоваться в browser/operator production path.

## 16. Production installation / handoff notes

Для production-like handoff:

1. Держать эту ветку отдельно; не merge в `main`.
2. Создать отдельный WhatsApp test account A, не личный основной номер.
3. Выдать отдельный instance token только для этого instance.
4. Сгенерировать новый `GLOBAL_API_KEY`; не использовать sample из `.env.example`.
5. Сгенерировать новый `CALL_STREAM_SIGNING_KEY`.
6. Хранить env в secret store или на сервере, не в Git.
7. Поднять PostgreSQL с отдельными DB для auth/users.
8. Запустить сервис с `CONNECT_ON_STARTUP=false` до контролируемого pairing.
9. Pair account A вручную через QR или pairing-code.
10. Запустить inbound live test и сохранить артефакты вне Git.
11. Запустить outbound live test и сохранить артефакты вне Git.
12. Записать итоговые PASS/FAIL, SHA, дату, кто подтвердил audible marker.
13. Только после PASS решать, нужен ли deploy/PR/merge/release.

Секреты, которые нельзя коммитить:

```text
.env
API keys
license codes
WhatsApp credentials
pairing codes
QR payloads/screenshots
session cookies
instance tokens
CALL_STREAM_SIGNING_KEY
real phone numbers in logs/artifacts
```

## 17. Exact next step

Следующий шаг ровно один:

```text
Pair account A, then run the live inbound/outbound WhatsApp call test with account B and validate PASS criteria from this document.
```

По-русски:

```text
Спарить WhatsApp account A, затем выполнить live inbound/outbound тест звонка с account B и проверить PASS-критерии выше.
```
