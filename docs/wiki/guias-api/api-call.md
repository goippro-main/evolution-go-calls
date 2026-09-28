# API de Chamadas

Documentação do endpoint para gerenciar chamadas WhatsApp.

## 📋 Índice

- [Rejeitar Chamada](#rejeitar-chamada)
- [Atender e transmitir áudio](#atender-e-transmitir-áudio)
- [Teste E2E ao vivo](#teste-e2e-ao-vivo)

---

## Rejeitar Chamada

Rejeita uma chamada recebida no WhatsApp.

**Endpoint**: `POST /call/reject`

**Headers**:
```
Content-Type: application/json
apikey: SUA-CHAVE-API
```

**Body**:
```json
{
  "callCreator": "5511999999999@s.whatsapp.net",
  "callId": "ABC123XYZ"
}
```

**Parâmetros**:

| Campo | Tipo | Obrigatório | Descrição |
|-------|------|-------------|-----------|
| `callCreator` | string (JID) | ✅ Sim | JID de quem está ligando |
| `callId` | string | ✅ Sim | ID da chamada |

**Nota**: Os dados da chamada (`callCreator` e `callId`) são recebidos via webhook quando uma chamada chega. Você deve capturar esses dados do evento `call` para usar este endpoint.

**Resposta de Sucesso (200)**:
```json
{
  "message": "success"
}
```

**Resposta de Erro (400)**:
```json
{
  "error": "invalid request body"
}
```

**Resposta de Erro (500)**:
```json
{
  "error": "instance not found"
}
```

**Exemplo cURL**:
```bash
curl -X POST http://localhost:4000/call/reject \
  -H "Content-Type: application/json" \
  -H "apikey: SUA-CHAVE-API" \
  -d '{
    "callCreator": "5511999999999@s.whatsapp.net",
    "callId": "ABC123XYZ"
  }'
```

---

## Atender e transmitir áudio

O fluxo de áudio é composto por três passos: capturar o `callId` do evento
`CallOffer`, atender a chamada e conectar o WebSocket dedicado. O serviço mantém
o registro por instância; um `callId` de outra instância nunca é aceito.

### Atender

```bash
curl -X POST http://localhost:4000/call/answer \
  -H "Content-Type: application/json" \
  -H "apikey: SUA-CHAVE-API" \
  -d '{"callCreator":"5511999999999@s.whatsapp.net","callId":"CALL_ID"}'
```

`404` significa que a oferta expirou, já terminou ou não pertence à instância.
Um erro de answer não remove o registro, permitindo nova tentativa enquanto a
chamada ainda estiver disponível.

### WebSocket de mídia

Para navegador ou operador, um backend confiável deve gerar uma URL HMAC curta
usando `CALL_STREAM_SIGNING_KEY`. A assinatura é o HMAC-SHA256 hexadecimal de:

```text
<instance>|<callId>|<exp>
```

Conecte usando:

```text
ws://localhost:4000/call/stream/CALL_ID?instance=INSTANCE&exp=UNIX_EXP&token=HMAC_HEX
```

`exp` deve estar dentro de cinco minutos e a tolerância de relógio é de cinco
segundos. Não entregue a chave API da instância ao navegador. O parâmetro
`apikey` continua disponível apenas para clientes legados não-browser.

Formato exato, em ambos os sentidos: mono PCM16 little-endian, 16 kHz, 960
samples por frame (60 ms), codificado em base64 dentro de JSON:

```json
{"event":"start","callId":"CALL_ID","sampleRate":16000}
{"event":"media","track":"inbound","payload":"..."}
{"event":"media","track":"outbound","sampleRate":16000,"payload":"..."}
{"event":"stop","reason":"hangup"}
```

`inbound` é áudio real decodificado recebido do interlocutor. `outbound` é
convertido para `float32` internamente e passado ao playout do meowcaller. Frames
recebidos antes de a chamada ficar `Active` são descartados; payloads inválidos
ou com tamanho diferente de `960 * 2` bytes encerram o stream.

### Encerrar

```bash
curl -X POST http://localhost:4000/call/hangup \
  -H "Content-Type: application/json" \
  -H "apikey: SUA-CHAVE-API" \
  -d '{"callId":"CALL_ID"}'
```

Fechamento do WebSocket também solicita hangup e remove o registro. Terminação
remota, disconnect e reconnect da instância limpam as chamadas pendentes.

### Dial outbound

`POST /call/dial` usa `meowcaller.Client.Call` e é experimental. O endpoint
retorna o `callId`, mas só deve ser considerado validado depois de confirmar no
dispositivo remoto tanto áudio recebido quanto áudio enviado pelo WebSocket.
O branch de produção é áudio-only; video e participant-add não fazem parte do
contrato.

## Teste E2E ao vivo

Requer uma instância pareada e um segundo telefone/conta WhatsApp. O teste
determinístico local cobre codec, autenticação, isolamento e gating, mas não pode
provar o relay criptografado do WhatsApp sem esses dispositivos.

1. Inicie o serviço com `CALL_STREAM_SIGNING_KEY` definido e conecte uma instância.
2. Faça uma chamada de voz do telefone de teste para a instância.
3. No evento `CallOffer`, salve `callId` e `callCreator`; chame `/call/answer`.
4. Gere a URL HMAC e conecte o WebSocket antes de falar. Grave os frames
   `inbound` em WAV 16 kHz mono e confirme voz inteligível, não apenas eventos
   `start`/`stop`.
5. Envie pelo menos dez frames `outbound` contendo uma onda PCM conhecida e
   confirme que o telefone remoto ouve a onda/voz. Depois repita com fala real.
6. Encerre pelo endpoint e confirme `stop`, fim da chamada nos dois telefones e
   que uma nova conexão ao mesmo `callId` recebe `404`.
7. Repita o stream com a credencial de outra instância e confirme `401`/`404`;
   nunca use a API key da instância no cliente browser.

Qualquer resultado baseado apenas em HTTP `200`, ringing ou conexão WebSocket
não é aprovação E2E de áudio bidirecional.

---

## Fluxo Completo de Rejeição Automática

### 1. Receber Evento de Chamada via Webhook

Quando alguém liga para sua instância, você recebe um webhook:

```json
{
  "event": "call",
  "instance": "minha-instancia",
  "data": {
    "id": "ABC123XYZ",
    "from": "5511999999999@s.whatsapp.net",
    "timestamp": "2025-11-11T10:30:00Z",
    "isVideo": false,
    "isGroup": false
  }
}
```

### 2. Rejeitar Automaticamente

No seu servidor que recebe webhooks, quando chegar um evento de chamada:
1. Capture o `id` e o `from` do evento
2. Faça uma requisição POST para `/call/reject`
3. Use os dados capturados como `callId` e `callCreator`

Dessa forma, chamadas são rejeitadas automaticamente assim que chegam.

### 3. Rejeição Seletiva

Para rejeitar apenas chamadas de números não autorizados:
1. Mantenha uma lista de números permitidos
2. Ao receber evento de chamada, verifique se o número está na lista
3. Se não estiver, rejeite a chamada usando o endpoint `/call/reject`
4. Se estiver autorizado, não faça nada (deixe tocar)

---

## Casos de Uso

### 1. Rejeitar Todas as Chamadas

Útil para contas de atendimento que só respondem via mensagens:
1. Configure seu webhook para receber eventos de chamada
2. Quando receber evento `call`, rejeite imediatamente
3. Envie uma mensagem de texto explicando que não atende chamadas

### 2. Horário Comercial

Rejeitar chamadas fora do horário de trabalho:
1. Ao receber evento de chamada, verifique o horário atual
2. Se estiver fora do horário comercial (ex: Segunda a Sexta, 9h-18h), rejeite
3. Envie mensagem informando o horário de atendimento

### 3. Rejeitar Chamadas de Vídeo

Aceitar apenas chamadas de áudio:
1. Verifique o campo `isVideo` no evento de chamada
2. Se for `true`, rejeite a chamada
3. Envie mensagem pedindo para ligar com chamada de voz

---

## Limitações e Observações

### Limitações do WhatsApp

1. **Relay experimental**: answer e mídia dependem da compatibilidade da versão
   pareada com whatsmeow/meowcaller; valide sempre com um telefone real.

2. **Chamadas em grupos e vídeo**: não fazem parte do contrato audio-only deste
   branch.

3. **Timing**: uma oferta pode expirar antes de `/call/answer` ser processado.

### Boas Práticas

1. **Sempre responda ao webhook rapidamente**: Rejeite a chamada em menos de 2 segundos para evitar timeout.

2. **Envie mensagem explicativa**: Após rejeitar, informe o usuário o motivo via mensagem de texto.

3. **Log de chamadas rejeitadas**: Mantenha registro para análise, salvando data/hora, número, ID da chamada e motivo da rejeição.

4. **Tratamento de erros**: Sempre trate possíveis erros na rejeição para evitar que seu webhook trave ao falhar em rejeitar uma chamada.

---

## Códigos de Erro Comuns

| Código | Erro | Solução |
|--------|------|---------|
| 400 | `invalid request body` | Verifique formato do JSON |
| 500 | `instance not found` | Instância não conectada |
| 500 | `error reject call` | Chamada não existe ou já expirou |

---

## Estrutura do Evento de Chamada

Quando você recebe um webhook de chamada, a estrutura é:

```json
{
  "event": "call",
  "instance": "minha-instancia",
  "data": {
    "id": "ABC123XYZ",
    "from": "5511999999999@s.whatsapp.net",
    "timestamp": "2025-11-11T10:30:00Z",
    "isVideo": false,
    "isGroup": false,
    "status": "ringing"
  }
}
```

**Campos**:
- `id`: ID único da chamada (use como `callId`)
- `from`: JID de quem está ligando (use como `callCreator`)
- `timestamp`: Quando a chamada foi iniciada
- `isVideo`: Se é chamada de vídeo (true) ou áudio (false)
- `isGroup`: Se é chamada em grupo
- `status`: Status da chamada (ringing, timeout, reject)

---

## Configuração de Webhooks

Para receber eventos de chamada, configure o webhook:

```env
WEBHOOK_URL=https://seu-servidor.com/webhook
```

Certifique-se de que seu servidor:
1. Aceita requisições POST
2. Responde rapidamente (< 5 segundos)
3. Retorna status 200 para confirmar recebimento

---

## Próximos Passos

- [Sistema de Eventos](../recursos-avancados/events-system.md) - Configurar webhooks
- [API de Mensagens](./api-messages.md) - Enviar mensagem após rejeitar
- [API de Usuários](./api-user.md) - Gerenciar contatos
- [Visão Geral da API](./api-overview.md)

---

**Documentação gerada para Evolution GO v1.0**
