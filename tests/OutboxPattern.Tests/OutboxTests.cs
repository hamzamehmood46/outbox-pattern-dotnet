using System.Collections.Concurrent;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Time.Testing;
using OutboxPattern;

namespace OutboxPattern.Tests;

public sealed class TestDb : IDisposable
{
    private readonly string _path = Path.Combine(Path.GetTempPath(), $"outbox-tests-{Guid.NewGuid():N}.db");
    public FakeTimeProvider Time { get; } = new(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
    public OutboxOptions Options { get; } = new() { BaseRetryDelay = TimeSpan.FromSeconds(2), MaxAttempts = 3, BatchSize = 50 };

    public TestDb()
    {
        using var db = NewContext();
        db.Database.EnsureCreated();
    }

    /// <summary>Each call returns a fresh context, like separate requests or separate service instances.</summary>
    public AppDbContext NewContext() =>
        new(new DbContextOptionsBuilder<AppDbContext>().UseSqlite($"Data Source={_path};Default Timeout=30").Options);

    public OutboxDispatcher Dispatcher(IMessagePublisher publisher, AppDbContext? db = null) =>
        new(db ?? NewContext(), publisher, Time, Options);

    public InvoiceService Invoices(AppDbContext? db = null) => new(db ?? NewContext(), Time);

    public void Dispose()
    {
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        try { File.Delete(_path); } catch (IOException) { /* best effort */ }
    }
}

public class RecordingPublisher : IMessagePublisher
{
    public ConcurrentQueue<Guid> Published { get; } = new();
    public int FailuresRemaining;
    public int Calls;

    public Task PublishAsync(OutboxMessage message, CancellationToken ct)
    {
        Interlocked.Increment(ref Calls);
        if (Interlocked.Decrement(ref FailuresRemaining) >= 0) throw new InvalidOperationException("broker unavailable");
        Published.Enqueue(message.Id);
        return Task.CompletedTask;
    }
}

public class OutboxTests : IDisposable
{
    private readonly TestDb _env = new();

    public void Dispose() => _env.Dispose();

    [Fact]
    public async Task Business_row_and_outbox_message_are_saved_together()
    {
        await _env.Invoices().CreateAsync("INV-1", "Acme", 100m);

        using var db = _env.NewContext();
        Assert.Equal(1, await db.Invoices.CountAsync());
        Assert.Equal(1, await db.Outbox.CountAsync());
    }

    [Fact]
    public async Task A_failed_business_write_leaves_no_orphan_message()
    {
        await _env.Invoices().CreateAsync("INV-1", "Acme", 100m);

        // Duplicate invoice number violates the unique index, so the whole SaveChanges must roll back.
        await Assert.ThrowsAsync<DbUpdateException>(() => _env.Invoices().CreateAsync("INV-1", "Acme", 100m));

        using var db = _env.NewContext();
        Assert.Equal(1, await db.Invoices.CountAsync());
        Assert.Equal(1, await db.Outbox.CountAsync()); // not 2: nothing was published about the failed invoice
    }

    [Fact]
    public async Task Pending_messages_are_published_once_and_marked_processed()
    {
        await _env.Invoices().CreateAsync("INV-1", "Acme", 100m);
        var publisher = new RecordingPublisher();

        Assert.Equal(1, await _env.Dispatcher(publisher).ProcessBatchAsync());
        Assert.Equal(0, await _env.Dispatcher(publisher).ProcessBatchAsync()); // nothing left

        Assert.Single(publisher.Published);
        using var db = _env.NewContext();
        Assert.NotNull((await db.Outbox.SingleAsync()).ProcessedAt);
    }

    [Fact]
    public async Task Messages_are_published_in_the_order_they_occurred()
    {
        var ids = new List<Guid>();
        foreach (var n in new[] { "A", "B", "C" })
        {
            await _env.Invoices().CreateAsync($"INV-{n}", "Acme", 10m);
            _env.Time.Advance(TimeSpan.FromSeconds(1));
        }
        using (var db = _env.NewContext())
            ids.AddRange(await db.Outbox.OrderBy(m => m.OccurredAt).Select(m => m.Id).ToListAsync());
        var publisher = new RecordingPublisher();

        await _env.Dispatcher(publisher).ProcessBatchAsync();

        Assert.Equal(ids, publisher.Published.ToList());
    }

    [Fact]
    public async Task Failures_are_retried_with_exponential_backoff()
    {
        await _env.Invoices().CreateAsync("INV-1", "Acme", 100m);
        var publisher = new RecordingPublisher { FailuresRemaining = 1 };

        Assert.Equal(0, await _env.Dispatcher(publisher).ProcessBatchAsync()); // fails, backs off 2s

        using (var db = _env.NewContext())
        {
            var message = await db.Outbox.SingleAsync();
            Assert.Equal(1, message.Attempts);
            Assert.Equal("broker unavailable", message.LastError);
            Assert.Null(message.LockedUntil);
        }

        Assert.Equal(0, await _env.Dispatcher(publisher).ProcessBatchAsync()); // not due yet
        Assert.Equal(1, publisher.Calls);

        _env.Time.Advance(TimeSpan.FromSeconds(2));
        Assert.Equal(1, await _env.Dispatcher(publisher).ProcessBatchAsync()); // now it succeeds
        Assert.Single(publisher.Published);
    }

    [Fact]
    public async Task Messages_are_dead_lettered_after_max_attempts()
    {
        await _env.Invoices().CreateAsync("INV-1", "Acme", 100m);
        var publisher = new RecordingPublisher { FailuresRemaining = int.MaxValue };

        for (var i = 0; i < _env.Options.MaxAttempts; i++)
        {
            await _env.Dispatcher(publisher).ProcessBatchAsync();
            _env.Time.Advance(TimeSpan.FromMinutes(10));
        }

        using var db = _env.NewContext();
        var message = await db.Outbox.SingleAsync();
        Assert.True(message.DeadLettered);
        Assert.Equal(_env.Options.MaxAttempts, message.Attempts);

        var callsBefore = publisher.Calls;
        await _env.Dispatcher(publisher).ProcessBatchAsync();
        Assert.Equal(callsBefore, publisher.Calls); // dead messages are left alone for an operator
    }

    [Fact]
    public async Task A_claimed_message_from_a_crashed_dispatcher_is_recovered_after_the_lock_expires()
    {
        await _env.Invoices().CreateAsync("INV-1", "Acme", 100m);
        using (var db = _env.NewContext())
        {
            // Simulate a dispatcher that claimed the message and then crashed before finishing.
            var lockedUntil = _env.Time.GetUtcNow().UtcDateTime + _env.Options.LockDuration;
            await db.Outbox.ExecuteUpdateAsync(s => s.SetProperty(m => m.LockedUntil, (DateTime?)lockedUntil));
        }
        var publisher = new RecordingPublisher();

        Assert.Equal(0, await _env.Dispatcher(publisher).ProcessBatchAsync()); // still reserved

        _env.Time.Advance(_env.Options.LockDuration + TimeSpan.FromSeconds(1));
        Assert.Equal(1, await _env.Dispatcher(publisher).ProcessBatchAsync()); // recovered
    }

    [Fact]
    public async Task Concurrent_dispatchers_never_publish_the_same_message_twice()
    {
        for (var i = 0; i < 40; i++) await _env.Invoices().CreateAsync($"INV-{i}", "Acme", 10m);
        var publisher = new RecordingPublisher();

        async Task Drain()
        {
            var dispatcher = _env.Dispatcher(publisher);
            while (await dispatcher.ProcessBatchAsync() > 0) { }
        }

        await Task.WhenAll(Drain(), Drain(), Drain());

        var published = publisher.Published.ToList();
        Assert.Equal(40, published.Count);
        Assert.Equal(40, published.Distinct().Count());
    }
}

public class IdempotentConsumerTests : IDisposable
{
    private readonly TestDb _env = new();

    public void Dispose() => _env.Dispose();

    private IdempotentConsumer Consumer() => new(_env.NewContext(), _env.Time);

    [Fact]
    public async Task Duplicate_deliveries_run_the_handler_only_once()
    {
        var id = Guid.NewGuid();
        var runs = 0;

        var first = await Consumer().HandleOnceAsync(id, "email-handler", () => { runs++; return Task.CompletedTask; });
        var second = await Consumer().HandleOnceAsync(id, "email-handler", () => { runs++; return Task.CompletedTask; });

        Assert.True(first);
        Assert.False(second);
        Assert.Equal(1, runs);
    }

    [Fact]
    public async Task Different_handlers_each_process_the_same_message()
    {
        var id = Guid.NewGuid();
        var runs = 0;

        await Consumer().HandleOnceAsync(id, "email-handler", () => { runs++; return Task.CompletedTask; });
        await Consumer().HandleOnceAsync(id, "ledger-handler", () => { runs++; return Task.CompletedTask; });

        Assert.Equal(2, runs);
    }

    [Fact]
    public async Task A_failing_handler_is_not_recorded_so_it_can_be_retried()
    {
        var id = Guid.NewGuid();

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            Consumer().HandleOnceAsync(id, "h", () => throw new InvalidOperationException("handler bug")));

        var retried = await Consumer().HandleOnceAsync(id, "h", () => Task.CompletedTask);

        Assert.True(retried);
    }

    [Fact]
    public async Task Concurrent_duplicate_deliveries_run_the_handler_exactly_once()
    {
        var id = Guid.NewGuid();
        var runs = 0;

        var results = await Task.WhenAll(Enumerable.Range(0, 6).Select(_ =>
            Consumer().HandleOnceAsync(id, "h", async () => { Interlocked.Increment(ref runs); await Task.Delay(20); })));

        Assert.Equal(1, runs);
        Assert.Equal(1, results.Count(r => r));
    }
}

