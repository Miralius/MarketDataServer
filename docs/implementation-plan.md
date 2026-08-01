# План реализации MarketDataServer

C++23-проект состоит из двух CMake targets: библиотека `market_data` содержит generated protobuf-код и прикладную логику, а `market_data_server` — конфигурацию и точку запуска. Зависимости: Boost.asio, Boost.lockfree, gRPC, Protobuf, PostgreSQL и libpqxx.

## 1. Сборка и protobuf

- Настроить CMake, зависимости и генерацию C++ из [`market_data.proto`](api/market_data.proto).
- Добавить в `market_data` валидацию моделей и Callback API для `Submit`, `GetResult`, `Health`.

## 2. PostgreSQL repository

- Реализовать работу с таблицами jobs и response chunks, включая статусы, ошибки и cache key.
- Реализовать через libpqxx state transitions, сохранение и чтение результатов.

## 3. Очередь и получение данных

- Добавить bounded `boost::lockfree::queue`, компактный `WorkItem` и worker pool.
- Изолировать provider-specific transport за асинхронным exchange adapter; polling выполнять таймерами Boost.Asio без блокировки workers.

## 4. Кэш, streaming

- Строить exact-match key из provider и нормализованного request payload без `client_request_id`; переиспользовать сохранённый результат.
- Хранить ответ chunks, отдавать metadata по gRPC.
- Добавить структурированные логи запросов, переходов состояния и ошибок без записи полного market-data payload.

## 5. Проверки

- Unit tests: validation, работу с key, state machine запроса, queue и порядок chunks.
- Эмуляторы: PostgreSQL, exchange adapter, успешный/ошибочный запрос, polling, restart recovery и последовательность cache miss → cache hit.
- _Нагрузочно проверить цели из нефункциональных требований и зафиксировать фактические latency и error rate._

Результат реализации - воспроизводимая сборка и документированный путь `Submit -> processing -> GetResult`, устойчивый к конкурентным запросам.
