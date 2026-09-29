# Подготовка реального WhatsApp audio E2E

## Точный flow Evolution Go fork

1. POST /instance/create требует apikey: GLOBAL_API_KEY. JSON содержит name
   и token; запись сохраняется в users DB. token становится ключом обычных
   instance API-запросов.
2. POST /instance/connect с instance token сохраняет webhook и подписки.
   Для звонков нужна подписка CALL.
3. Для новой сессии GET /instance/qr запускает whatsmeow, ждёт QR и
   возвращает qrcode/code. QR сканируется в WhatsApp: Настройки →
   Связанные устройства → Связать устройство.
4. Альтернатива — POST /instance/pair с {"phone":"<номер>"}. Fork вызывает
   PairPhone; полученный pairingCode вводится на телефоне через Связанные
   устройства → Связать устройство по номеру телефона.
5. meowcaller.NewClient создаётся до client.Connect(). При входящем звонке
   OnIncomingCall помещает живой Call в registry. После POST /call/answer
   открывается GET /call/stream/:callId.

QR и pairing code меняют внешний WhatsApp account. Harness их не запускает.

## Runtime dependencies

Для локального процесса нужны Go 1.25+, PostgreSQL для users DB и AUTH DB,
а также GLOBAL_API_KEY, DATABASE_SAVE_MESSAGES, SERVER_PORT и CLIENT_NAME.
AMQP, NATS, MinIO и proxy не нужны для минимального call E2E.
Для HMAC stream нужен CALL_STREAM_SIGNING_KEY; тот же ключ должен быть у
сервиса и harness. Он не является instance token.

Минимальные значения задаются оператором только в локальном окружении:

    SERVER_PORT=4000
    POSTGRES_AUTH_DB=postgresql://.../evogo_auth?sslmode=disable
    POSTGRES_USERS_DB=postgresql://.../evogo_users?sslmode=disable
    DATABASE_SAVE_MESSAGES=false
    GLOBAL_API_KEY=<локальный admin key>
    CLIENT_NAME=evolution
    CONNECT_ON_STARTUP=false
    CALL_STREAM_SIGNING_KEY=<локальный stream key>

go test ./... не требует PostgreSQL и не доказывает live WhatsApp media.
При выключенном Docker daemon checkout можно собрать и тестировать, но
сервис не поднимется без доступного PostgreSQL.

## Pairing commands

Создать instance, только если он ещё не создан:

    curl -X POST "$API/instance/create" \
      -H "apikey: $GLOBAL_API_KEY" -H "Content-Type: application/json" \
      -d '{"name":"wa-call-e2e","token":"<instance-token>"}'

QR flow:

    curl -X POST "$API/instance/connect" -H "apikey: $INSTANCE_TOKEN" \
      -H "Content-Type: application/json" -d '{"subscribe":["CALL"]}'
    curl "$API/instance/qr" -H "apikey: $INSTANCE_TOKEN"

Pairing-code flow:

    curl -X POST "$API/instance/pair" -H "apikey: $INSTANCE_TOKEN" \
      -H "Content-Type: application/json" \
      -d '{"phone":"<international number without +>"}'

Проверка:

    curl "$API/instance/status" -H "apikey: $INSTANCE_TOKEN"

После pairing запустить harness на машине, видимой Evolution webhook:

    EVOLUTION_API_URL="$API" \
    EVOLUTION_INSTANCE="wa-call-e2e" \
    EVOLUTION_INSTANCE_API_KEY="$INSTANCE_TOKEN" \
    CALL_STREAM_SIGNING_KEY="$CALL_STREAM_SIGNING_KEY" \
    go run ./tools/whatsapp-call-e2e \
      -webhook-url http://127.0.0.1:8090/webhook

Если Evolution в контейнере, используйте, например,
-webhook-url http://host.docker.internal:8090/webhook и
-webhook-listen 0.0.0.0:8090.

## Единственное действие человека для live E2E

После подготовки уже спаренного dedicated test number позвонить с отдельного
согласованного WhatsApp номера. Harness ответит, запишет inbound track,
отправит marker и сделает hangup. Если pairing ещё не выполнен, отдельное
действие человека — сканировать QR или ввести pairing code; без явного
подтверждения владельца аккаунта это не выполняется.

Успех нельзя объявлять по HTTP 200, ringing или ws_start. Нужны
положительные inbound frames, nonZeroFrames > 0, прослушиваемый WAV и
подтверждение, что удалённый телефон слышит marker 440/880/660 Hz. Нулевые
или только silent frames — отсутствие подтверждённого аудио.

## Два участника и отдельные tracks

Текущий bridge — один WhatsApp 1:1 call: одна inbound дорожка от удалённого
участника и один outbound playout обратно в тот же call. Registry, webhook и
media protocol идентифицируют один callId. Group-call negotiation,
participant identity и отдельные tracks сейчас отсутствуют.

Для двух согласных португалоговорящих участников потребуется отдельная
архитектура: group-call transport, стабильный participant identity в каждом
frame, раздельные PCM buffers/WAV tracks, общие timestamps и consent/retention
policy. Текущий audio-only 1:1 bridge не может сохранить две независимые
нативные дорожки и не реализует group calling.
