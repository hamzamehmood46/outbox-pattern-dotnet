using System.Text.Json;
using Microsoft.EntityFrameworkCore;

namespace OutboxPattern;

public interface IMessagePublisher
{
    /// <summary>Publish to the real broker (Service Bus, RabbitMQ, Kafka...). Throw to signal failure.</summary>
    Task PublishAsync(OutboxMessage message, CancellationToken ct);
}

public class OutboxOptions
{
    public int BatchSize { get; set; } = 20;
    public int MaxAttempts { get; set; } = 5;
    public TimeSpan BaseRetryDelay { get; set; } = TimeSpan.FromSeconds(2);
    public TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>How long a claimed message is reserved. If a dispatcher crashes, the message is retried after this.</summary>
    public TimeSpan LockDuration { get; set; } = TimeSpan.FromSeconds(30);
}

/// <summary>Business operation and outbox write happen in one SaveChanges, i.e. one atomic transaction.</summary>
public class InvoiceService(AppDbContext db, TimeProvider time)
{
    public async Task<Invoice> CreateAsync(string number, string customer, decimal amount, CancellationToken ct = default)
    {
        var invoice = new Invoice(number, customer, amount);
        db.Invoices.Add(invoice);

        var now = time.GetUtcNow().UtcDateTime;
        db.Outbox.Add(new OutboxMessage
        {
            Type = nameof(InvoiceCreated),
            Payload = JsonSerializer.Serialize(new InvoiceCreated(invoice.Id, number, amount)),
            OccurredAt = now,
            NextAttemptAt = now
        });

        await db.SaveChangesAsync(ct); // both rows commit together, or neither does
        return invoice;
    }
}

/// <summary>
/// Publishes pending outbox messages: at-least-once, oldest first, safe to run from several instances at once.
/// Delivery is at-least-once (a crash after publishing but before marking it processed causes a re-send),
/// so consumers must be idempotent - see <see cref="IdempotentConsumer"/>.
/// </summary>
public class OutboxDispatcher(AppDbContext db, IMessagePublisher publisher, TimeProvider time, OutboxOptions options)
{
    /// <summary>Processes one batch and returns how many messages were published successfully.</summary>
    public async Task<int> ProcessBatchAsync(CancellationToken ct = default)
    {
        var now = time.GetUtcNow().UtcDateTime;

        var due = await db.Outbox.AsNoTracking()
            .Where(m => m.ProcessedAt == null && !m.DeadLettered && m.NextAttemptAt <= now
                        && (m.LockedUntil == null || m.LockedUntil <= now))
            .OrderBy(m => m.OccurredAt).ThenBy(m => m.Id)
            .Take(options.BatchSize)
            .ToListAsync(ct);

        var published = 0;
        foreach (var message in due)
        {
            if (!await TryClaimAsync(message.Id, now, ct)) continue; // another dispatcher got it

            try
            {
                await publisher.PublishAsync(message, ct);
                await MarkProcessedAsync(message.Id, ct);
                published++;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                await RecordFailureAsync(message, ex, ct);
            }
        }

        return published;
    }

    /// <summary>Atomic claim: the UPDATE only succeeds for one caller, so two dispatchers never publish the same message.</summary>
    private async Task<bool> TryClaimAsync(Guid id, DateTime now, CancellationToken ct)
    {
        var lockedUntil = now + options.LockDuration;
        var rows = await db.Outbox
            .Where(m => m.Id == id && m.ProcessedAt == null && (m.LockedUntil == null || m.LockedUntil <= now))
            .ExecuteUpdateAsync(s => s.SetProperty(m => m.LockedUntil, (DateTime?)lockedUntil), ct);
        return rows == 1;
    }

    private Task MarkProcessedAsync(Guid id, CancellationToken ct)
    {
        var processedAt = time.GetUtcNow().UtcDateTime;
        return db.Outbox.Where(m => m.Id == id).ExecuteUpdateAsync(s => s
            .SetProperty(m => m.ProcessedAt, (DateTime?)processedAt)
            .SetProperty(m => m.LockedUntil, (DateTime?)null)
            .SetProperty(m => m.LastError, (string?)null), ct);
    }

    private Task RecordFailureAsync(OutboxMessage message, Exception ex, CancellationToken ct)
    {
        var attempts = message.Attempts + 1; // we hold the claim, so this is race-free
        var dead = attempts >= options.MaxAttempts;
        var delay = TimeSpan.FromMilliseconds(Math.Min(
            options.MaxRetryDelay.TotalMilliseconds,
            options.BaseRetryDelay.TotalMilliseconds * Math.Pow(2, attempts - 1)));
        var next = time.GetUtcNow().UtcDateTime + delay;
        var error = ex.Message.Length > 500 ? ex.Message[..500] : ex.Message;

        return db.Outbox.Where(m => m.Id == message.Id).ExecuteUpdateAsync(s => s
            .SetProperty(m => m.Attempts, attempts)
            .SetProperty(m => m.NextAttemptAt, next)
            .SetProperty(m => m.LockedUntil, (DateTime?)null)
            .SetProperty(m => m.LastError, error)
            .SetProperty(m => m.DeadLettered, dead), ct);
    }
}

/// <summary>
/// Runs a handler at most once per (message, handler), even if the broker delivers the message twice.
/// The inbox row and the handler's own changes commit in one transaction.
/// </summary>
public class IdempotentConsumer(AppDbContext db, TimeProvider time)
{
    /// <returns>True if the handler ran; false if this message was already processed.</returns>
    public async Task<bool> HandleOnceAsync(Guid messageId, string handlerName, Func<Task> handler, CancellationToken ct = default)
    {
        if (await db.Inbox.AnyAsync(i => i.MessageId == messageId && i.Handler == handlerName, ct)) return false;

        await using var transaction = await db.Database.BeginTransactionAsync(ct);
        try
        {
            // Claim the message FIRST. The inbox primary key makes this insert fail for every concurrent
            // duplicate, so the handler below never starts twice. If the handler throws, the transaction
            // rolls the claim back and the message can be retried.
            db.Inbox.Add(new InboxMessage { MessageId = messageId, Handler = handlerName, ProcessedAt = time.GetUtcNow().UtcDateTime });
            await db.SaveChangesAsync(ct);

            await handler();

            await db.SaveChangesAsync(ct);
            await transaction.CommitAsync(ct);
            return true;
        }
        catch (DbUpdateException)
        {
            // A concurrent delivery of the same message won the race to insert the inbox row.
            db.ChangeTracker.Clear();
            if (await db.Inbox.AnyAsync(i => i.MessageId == messageId && i.Handler == handlerName, ct)) return false;
            throw;
        }
    }
}
