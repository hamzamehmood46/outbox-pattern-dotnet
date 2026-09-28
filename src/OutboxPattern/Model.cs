using Microsoft.EntityFrameworkCore;

namespace OutboxPattern;

/// <summary>Sample business entity. The point is what happens *next to* it, not the entity itself.</summary>
public class Invoice
{
    public Guid Id { get; private set; } = Guid.NewGuid();
    public string Number { get; private set; } = string.Empty;
    public string Customer { get; private set; } = string.Empty;
    public decimal Amount { get; private set; }

    private Invoice() { }

    public Invoice(string number, string customer, decimal amount)
    {
        Number = number;
        Customer = customer;
        Amount = amount;
    }
}

public record InvoiceCreated(Guid InvoiceId, string Number, decimal Amount);

/// <summary>
/// A message waiting to be published. It is written in the SAME transaction as the business change,
/// so either both are saved or neither is. A separate dispatcher publishes it afterwards.
/// </summary>
public class OutboxMessage
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public string Type { get; set; } = string.Empty;
    public string Payload { get; set; } = string.Empty;
    public DateTime OccurredAt { get; set; }
    public DateTime? ProcessedAt { get; set; }
    public int Attempts { get; set; }
    public DateTime NextAttemptAt { get; set; }
    public DateTime? LockedUntil { get; set; }
    public string? LastError { get; set; }
    public bool DeadLettered { get; set; }
}

/// <summary>Records which messages a handler has already processed, making consumers idempotent.</summary>
public class InboxMessage
{
    public Guid MessageId { get; set; }
    public string Handler { get; set; } = string.Empty;
    public DateTime ProcessedAt { get; set; }
}

public class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
{
    public DbSet<Invoice> Invoices => Set<Invoice>();
    public DbSet<OutboxMessage> Outbox => Set<OutboxMessage>();
    public DbSet<InboxMessage> Inbox => Set<InboxMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Invoice>(e =>
        {
            e.HasKey(i => i.Id);
            e.HasIndex(i => i.Number).IsUnique();
            e.Property(i => i.Number).HasMaxLength(50).IsRequired();
            e.Property(i => i.Amount).HasPrecision(18, 2);
        });

        modelBuilder.Entity<OutboxMessage>(e =>
        {
            e.ToTable("Outbox");
            e.HasKey(m => m.Id);
            e.Property(m => m.Type).HasMaxLength(200).IsRequired();
            // The dispatcher's hot query: unprocessed, not dead, due now.
            e.HasIndex(m => new { m.ProcessedAt, m.DeadLettered, m.NextAttemptAt });
        });

        modelBuilder.Entity<InboxMessage>(e =>
        {
            e.ToTable("Inbox");
            e.HasKey(m => new { m.MessageId, m.Handler });
        });
    }
}
