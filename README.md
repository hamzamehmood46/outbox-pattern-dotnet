# Transactional Outbox

[![CI](https://github.com/hamzamehmood46/outbox-pattern-dotnet/actions/workflows/ci.yml/badge.svg)](https://github.com/hamzamehmood46/outbox-pattern-dotnet/actions/workflows/ci.yml)

A **.NET 8 / EF Core** implementation of the **transactional outbox** and **idempotent inbox** patterns: the reliable way to update a database *and* publish an event without ever doing one but not the other.

## The problem

```csharp
await db.SaveChangesAsync();          // 1. save the invoice
await broker.PublishAsync(event);     // 2. tell everyone   <-- crash here and the event is lost forever
```

Swap the order and you can announce something that never got saved. A database and a message broker can't share a transaction, so one of them will eventually disagree with the other.

## The solution

```
 ┌──────────────── one DB transaction ────────────────┐
 │  INSERT Invoice        INSERT Outbox(message)      │   <- atomic: both or neither
 └────────────────────────────────────────────────────┘
                              │
              OutboxDispatcher (polls, claims, publishes)
                              │  at-least-once
                              ▼
                        message broker
                              │
                              ▼
        IdempotentConsumer: INSERT Inbox(messageId) + handler, in one transaction
                              │  duplicate delivery -> handler skipped
                              ▼
                     effect happens exactly once
```

**At-least-once delivery + idempotent consumers = effectively exactly-once.** That is the practical guarantee real systems build on.

## What's implemented

| Concern | How |
|---|---|
| **Atomicity** | `InvoiceService` writes the invoice and its outbox row in a single `SaveChanges` (one transaction) |
| **Ordering** | Messages are published oldest-first |
| **Retries** | Exponential backoff via `NextAttemptAt`; failures record `LastError` and `Attempts` |
| **Poison messages** | After `MaxAttempts`, a message is `DeadLettered` and left for an operator |
| **Multiple dispatchers** | Each message is claimed with an atomic conditional `UPDATE` (`ExecuteUpdate`), so instances never publish the same message twice |
| **Crash recovery** | A claim is a time-limited lease (`LockDuration`); if a dispatcher dies mid-publish, another picks the message up after it expires |
| **Idempotent consumers** | `IdempotentConsumer` claims an inbox row *first*, then runs the handler, in one transaction. Concurrent duplicates lose the insert and never start the handler. A failing handler rolls the claim back so it can be retried. |
| **Testability** | All time comes from `TimeProvider`; tests use `FakeTimeProvider`, so backoff and lease expiry are tested instantly |

## Usage

```csharp
services.AddDbContext<AppDbContext>(o => o.UseSqlite("Data Source=app.db"));
services.AddSingleton(TimeProvider.System);
services.AddSingleton(new OutboxOptions { MaxAttempts = 5 });
services.AddSingleton<IMessagePublisher, MyServiceBusPublisher>();   // your broker
services.AddScoped<OutboxDispatcher>();
services.AddScoped<InvoiceService>();
services.AddHostedService(sp => new OutboxWorker(sp.GetRequiredService<IServiceScopeFactory>()));
```

## Tests

```bash
dotnet test
```

12 tests, run against a real SQLite database:

- business row and outbox row commit together; a failed write leaves **no orphan message**
- publish once, in order; backoff schedule; dead-lettering after max attempts
- **crash recovery** after lease expiry
- **3 concurrent dispatchers, 40 messages: each published exactly once**
- idempotent consumer: duplicates skipped, per-handler tracking, failed handlers retryable, **6 concurrent duplicate deliveries run the handler once**

> While writing the concurrency test, the first version of `IdempotentConsumer` ran the handler *before* claiming the inbox row, so simultaneous duplicates both executed it. The fix (claim first, inside the transaction) is documented in the code.

## Production notes

- SQLite is used so the tests run anywhere. On SQL Server/PostgreSQL, claim with `UPDLOCK, READPAST` / `FOR UPDATE SKIP LOCKED` for better throughput.
- Prune processed outbox/inbox rows on a schedule.
- Replace the polling worker with change-data-capture (e.g. Debezium) at very high volume.

## License

MIT
