# План реализации MarketDataServer

C++23-проект состоит из двух CMake targets: библиотека `market_data` содержит generated protobuf-код и прикладную логику, а `market_data_server` — конфигурацию и точку запуска. Зависимости: Boost.asio, Boost.lockfree, gRPC, Protobuf, PostgreSQL и libpqxx.

## 1. Сборка и protobuf

- Настроить CMake, зависимости и генерацию C++ из [`market_data.proto`](api/market_data.proto).
- Добавить в `market_data` валидацию моделей и Callback API для `Submit`, `GetResult`, `Health`.

## 2. Структурированный логгер

- Определить структурированные события для входящих RPC, переходов состояния, cache disposition, provider latency, retry и terminal outcome; передавать в них `client_request_id`/`job_id`, provider, operation, old/new status, retry number и error code.
- Не записывать полный market-data payload и предусмотреть подмену sink в тестах, чтобы дальнейшие этапы сразу получали пригодную для отладки трассировку.

## 3. PostgreSQL repository

- Реализовать работу с таблицами jobs и response chunks, включая статусы, ошибки, cache key и счётчик последовательных transient retries.
- Реализовать через libpqxx state transitions, сохранение и чтение результатов.

## 4. Очередь и получение данных

- Добавить bounded `boost::lockfree::queue`, компактный `WorkItem` и worker pool.
- Изолировать provider-specific transport за асинхронным exchange adapter; polling и transient retry выполнять таймерами Boost.Asio без блокировки workers.
- Добавить runtime-параметры `poll_interval` (по умолчанию 1 s) и `max_transient_retries` (по умолчанию 3). Повторять сетевые/транспортные ошибки, включая `UNAVAILABLE`, с тем же идемпотентным ключом; успешным ответом сбрасывать счётчик, а permanent/protocol errors завершать без retry.
- При transient-обрыве provider stream удалять сохранённые чанки и повторять чтение с chunk 0; общий deadline проверять перед каждым планированием retry.

## 5. Кэш и streaming

- Строить exact-match key из provider и нормализованного request payload без `client_request_id`; переиспользовать сохранённый результат.
- Хранить ответ chunks, отдавать metadata по gRPC.
- Подключить события logger facade для cache hit/miss, сохранения chunks и завершения stream.

## 6. Проверки

- Unit tests: validation, работу с key, state machine запроса, queue и порядок chunks.
- Unit tests retry policy с контролируемыми часами: интервал из конфигурации, сброс счётчика после успеха, приоритет deadline и отсутствие retry для permanent/protocol errors.
- Эмуляторы: PostgreSQL, exchange adapter, успешный/ошибочный запрос, polling, transient `UNAVAILABLE`, исчерпание трёх retry, replay stream с chunk 0, restart recovery и последовательность cache miss → cache hit.
- Проверить через тестовый log sink наличие событий переходов состояния и retry без полного market-data payload.
- _Нагрузочно проверить цели из нефункциональных требований и зафиксировать фактические latency и error rate._

Результат реализации - воспроизводимая сборка и документированный путь `Submit -> processing -> GetResult`, устойчивый к конкурентным запросам.
