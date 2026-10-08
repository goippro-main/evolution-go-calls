# WhatsApp voice calls Evolution Go — состояние 2026-10-08

## Цель
Два WhatsApp-аккаунта, тест двустороннего аудио и запись раздельных дорожек pt-PT для Moshi. Первый аккаунт Samsung A10 +351 920 711 655 подключён к локальному Evolution Go на Mac; второй обычный WhatsApp-абонент, QR не нужен. Португальский egress PT/MEO/Maia обязателен; нельзя переносить трафик на иностранные серверы.

## Код и инфраструктура
GitHub: https://github.com/goippro-main/evolution-go-calls ; рабочая ветка codex/whatsapp-voice-calls (не сливать в main до живого E2E). Mac: /private/tmp/evolution-go-calls, API localhost:4000, instance wa-call-e2e, webhook localhost:8090, harness /private/tmp/whatsapp-call-e2e-fixed; launchd com.valera.wa-call-e2e-harness; логи /private/tmp/wa-call-e2e-harness-live/artifacts/lifecycle.jsonl. Успешно проверялись Connected/LoggedIn, webhook CALL, PT IP, Go tests/build, два synthetic offers (не реальная media).

## Что сделали и наблюдали
08.10 несколько реальных тестов: первые звонки сбрасываются немедленно, последующие Ringing без ответа. В первом тесте CallOffer поступал, /call/answer 200, WS /call/stream 200, но не достигалось active и inboundFrames=0, WAV 44B. Первый harness был one-shot, выходил после early stream close (~0.6 s); stream close вызывал cleanup/hangup. После его выхода webhook 8090 отказывал и следующие звонки оставались Ringing. Codex исправил harness: persistent listener, sync bind, -max-calls, подробные ws_close логи, regression test на два failed streams; тесты и build прошли. Deploy через launchd, webhook и два synthetic CallOffer прошли, но третий живой тест вновь провалился: два немедленных drop, третий Ringing. Значит media/signaling root cause НЕ исправлен, READY/PASS заявлять нельзя.

## Решения
Не перепривязывать первый телефон; не звонить повторно без доказательного анализа логов; не трогать outbound, чужие серверы или IP; не утверждать успех по HTTP 200, synthetic webhook или открытому порту. Успех: реальный call Active, nonzero inbound/outbound audio frames, валидный WAV и слышимый marker 440/880/660Hz.

## Открыто
Точное происхождение early hangup, ICE/DTLS/WebRTC/WhatsApp negotiation, жизненный цикл listener после нескольких реальных offers, реальная передача аудио. Codex job ajob-1791465687-efc6 выполнял глубокую диагностику, на момент записи окончательный результат ещё не получен. Проверить его результат перед продолжением.

## Следующие действия
1. Получить финальный отчёт ajob-1791465687-efc6 и корреляцию CallOffer→Answer→WS→media→hangup для 3 последних вызовов.
2. Исправить конкретную причину раннего закрытия stream/отсутствия Active, покрыть тестами, развернуть и проверить стабильность listener.
3. Только после этого запросить один контролируемый живой тест с согласия участников, затем проверить двустороннее аудио и артефакты.
