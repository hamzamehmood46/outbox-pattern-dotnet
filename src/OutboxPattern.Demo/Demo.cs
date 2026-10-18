using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;

namespace OutboxPattern.Demo;

/// <summary>The real clock plus an offset, so a visitor can skip past a retry delay instead of waiting.</summary>
public sealed class SkewedClock : TimeProvider
{
    private long _offsetTicks;

    public override DateTimeOffset GetUtcNow() => base.GetUtcNow() + TimeSpan.FromTicks(Interlocked.Read(ref _offsetTicks));

    public DateTimeOffset Skip(TimeSpan by)
    {
        Interlocked.Add(ref _offsetTicks, by.Ticks);
        return GetUtcNow();
    }
}

public enum BrokerMode
{
    /// <summary>Accepts every message.</summary>
    Healthy,

    /// <summary>Refuses every message, like a broker outage.</summary>
    Down,

    /// <summary>Takes delivery, then the acknowledgement is lost, so the sender will send it again.</summary>
    LoseReply,
}

public sealed record Delivery(Guid MessageId, DateTime At, bool Duplicate, bool EffectApplied);

/// <summary>
/// A stand-in for Service Bus, RabbitMQ or Kafka, and the consumer behind it. Each delivery goes through the
/// library's <see cref="IdempotentConsumer"/>, so the effect (here, "send the invoice email") happens once per
/// message however many times the broker delivers it.
/// </summary>
public sealed class DemoBroker : IMessagePublisher
{
    private readonly IServiceScopeFactory _scopes;
    private readonly TimeProvider _time;
    private readonly object _gate = new();
    private readonly HashSet<Guid> _seen = [];
    private readonly List<Delivery> _deliveries = [];
    private readonly ConcurrentDictionary<Guid, int> _emails = new();
    private readonly HashSet<Guid> _lostOnce = [];

    public DemoBroker(IServiceScopeFactory scopes, TimeProvider time)
    {
        _scopes = scopes;
        _time = time;
    }

    public BrokerMode Mode { get; set; } = BrokerMode.Healthy;

    public IReadOnlyList<Delivery> Deliveries
    {
        get
        {
            lock (_gate)
            {
                return _deliveries.ToList();
            }
        }
    }

    /// <summary>How many invoice emails were actually sent, per invoice. This is the real-world effect, and should be 1.</summary>
    public IReadOnlyDictionary<Guid, int> Emails => _emails;

    public async Task PublishAsync(OutboxMessage message, CancellationToken ct)
    {
        if (Mode == BrokerMode.Down)
        {
            throw new InvalidOperationException("Broker unavailable (503)");
        }

        var duplicate = false;
        lock (_gate)
        {
            duplicate = !_seen.Add(message.Id);
        }

        var invoice = JsonSerializer.Deserialize<InvoiceCreated>(message.Payload)!;
        using var scope = _scopes.CreateScope();
        var consumer = scope.ServiceProvider.GetRequiredService<IdempotentConsumer>();
        var applied = await consumer.HandleOnceAsync(message.Id, "send-invoice-email", () =>
        {
            _emails.AddOrUpdate(invoice.InvoiceId, 1, (_, n) => n + 1);
            return Task.CompletedTask;
        }, ct);

        lock (_gate)
        {
            _deliveries.Add(new Delivery(message.Id, _time.GetUtcNow().UtcDateTime, duplicate, applied));
        }

        if (Mode == BrokerMode.LoseReply)
        {
            bool first;
            lock (_gate)
            {
                first = _lostOnce.Add(message.Id);
            }

            if (first)
            {
                throw new TimeoutException("Timed out waiting for the broker's acknowledgement (the message may have arrived)");
            }
        }
    }
}
