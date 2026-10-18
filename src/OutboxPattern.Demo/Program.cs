using Microsoft.EntityFrameworkCore;
using Microsoft.OpenApi.Models;
using OutboxPattern;
using OutboxPattern.Demo;

var builder = WebApplication.CreateBuilder(args);

var clock = new SkewedClock();
builder.Services.AddSingleton(clock);
builder.Services.AddSingleton<TimeProvider>(clock);
builder.Services.AddDbContext<AppDbContext>(o => o.UseSqlite(builder.Configuration.GetConnectionString("Default") ?? "Data Source=outbox-demo.db;Default Timeout=30"));
builder.Services.AddSingleton(new OutboxOptions { BaseRetryDelay = TimeSpan.FromSeconds(5), MaxAttempts = 4, BatchSize = 50 });
builder.Services.AddSingleton<DemoBroker>();
builder.Services.AddSingleton<IMessagePublisher>(sp => sp.GetRequiredService<DemoBroker>());
builder.Services.AddScoped<InvoiceService>();
builder.Services.AddScoped<OutboxDispatcher>();
builder.Services.AddScoped<IdempotentConsumer>();
if (builder.Configuration.GetValue("Worker:Enabled", true))
{
    builder.Services.AddHostedService(sp => new OutboxWorker(sp.GetRequiredService<IServiceScopeFactory>(), TimeSpan.FromSeconds(1)));
}

builder.Services.ConfigureHttpJsonOptions(o => o.SerializerOptions.Converters.Add(new System.Text.Json.Serialization.JsonStringEnumConverter()));
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(o => o.SwaggerDoc("v1", new OpenApiInfo
{
    Title = "Transactional Outbox",
    Version = "v1",
    Description = "Save an invoice and its event in one transaction, publish reliably to a simulated broker, and consume idempotently.",
}));

var app = builder.Build();
using (var scope = app.Services.CreateScope())
{
    scope.ServiceProvider.GetRequiredService<AppDbContext>().Database.EnsureCreated();
}

app.UseSwagger();
app.UseSwaggerUI();
app.MapGet("/", () => Results.Redirect("/swagger")).ExcludeFromDescription();

var api = app.MapGroup("/api");

// The invoice and its outbox message are written in one SaveChanges, so both exist or neither does.
api.MapPost("/invoices", async (NewInvoice body, InvoiceService invoices) =>
{
    if (string.IsNullOrWhiteSpace(body.Number) || string.IsNullOrWhiteSpace(body.Customer) || body.Amount <= 0)
    {
        return Results.BadRequest(new { error = "Number, customer and a positive amount are required." });
    }

    try
    {
        var invoice = await invoices.CreateAsync(body.Number.Trim(), body.Customer.Trim(), body.Amount);
        return Results.Created($"/api/invoices/{invoice.Id}", new { invoice.Id, invoice.Number, invoice.Customer, invoice.Amount, note = "An InvoiceCreated message is now waiting in the outbox." });
    }
    catch (DbUpdateException)
    {
        return Results.Conflict(new { error = $"Invoice number {body.Number} already exists. Nothing was saved: no invoice and no outbox message." });
    }
});

api.MapGet("/invoices", async (AppDbContext db) =>
    await db.Invoices.AsNoTracking().OrderBy(i => i.Number).Select(i => new { i.Id, i.Number, i.Customer, i.Amount }).ToListAsync());

api.MapGet("/outbox", async (AppDbContext db) =>
{
    var rows = await db.Outbox.AsNoTracking().OrderBy(m => m.OccurredAt).ToListAsync();
    return rows.Select(m => new
    {
        m.Id,
        m.Type,
        status = m.DeadLettered ? "DeadLettered" : m.ProcessedAt is not null ? "Published" : m.Attempts > 0 ? "Retrying" : "Pending",
        m.Attempts,
        m.NextAttemptAt,
        m.LastError,
        m.ProcessedAt,
    });
});

api.MapPost("/outbox/{id:guid}/retry", async (Guid id, AppDbContext db, TimeProvider time) =>
{
    var rows = await db.Outbox.Where(m => m.Id == id && m.DeadLettered)
        .ExecuteUpdateAsync(s => s
            .SetProperty(m => m.DeadLettered, false)
            .SetProperty(m => m.Attempts, 0)
            .SetProperty(m => m.NextAttemptAt, time.GetUtcNow().UtcDateTime)
            .SetProperty(m => m.LastError, (string?)null));
    return rows == 1 ? Results.Accepted() : Results.Conflict(new { error = "Only dead-lettered messages can be retried." });
});

api.MapPost("/worker/run", async (OutboxDispatcher dispatcher) => Results.Ok(new { published = await dispatcher.ProcessBatchAsync() }));

// ---- The simulated broker and its consumer ----
api.MapGet("/broker", (DemoBroker broker) => Results.Ok(new { mode = broker.Mode.ToString(), modes = Enum.GetNames<BrokerMode>() }));

api.MapPost("/broker/mode", (ModeRequest body, DemoBroker broker) =>
{
    broker.Mode = body.Mode;
    return Results.Ok(new { mode = broker.Mode.ToString() });
});

api.MapGet("/broker/deliveries", (DemoBroker broker) => broker.Deliveries.OrderBy(d => d.At));

api.MapGet("/effects", (DemoBroker broker) => Results.Ok(new
{
    note = "Invoice emails actually sent. Each invoice should appear exactly once, whatever the broker did.",
    emailsSent = broker.Emails.Select(e => new { invoiceId = e.Key, count = e.Value }),
}));

api.MapPost("/demo/skip", (SkipRequest body, SkewedClock c) =>
    Results.Ok(new { now = c.Skip(TimeSpan.FromSeconds(Math.Clamp(body.Seconds, 0, 86_400))) }));

app.Run();

public record NewInvoice(string? Number, string? Customer, decimal Amount);
public record ModeRequest(BrokerMode Mode);
public record SkipRequest(int Seconds);

public partial class Program;
