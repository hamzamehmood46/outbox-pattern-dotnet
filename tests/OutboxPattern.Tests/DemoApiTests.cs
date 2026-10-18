using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Data.Sqlite;

namespace OutboxPattern.Tests;

public class DemoApiTests : IDisposable
{
    private readonly string _db = Path.Combine(Path.GetTempPath(), $"outbox-demo-{Guid.NewGuid():N}.db");
    private readonly WebApplicationFactory<Program> _app;
    private readonly HttpClient _client;

    public DemoApiTests()
    {
        _app = new WebApplicationFactory<Program>().WithWebHostBuilder(b =>
        {
            b.UseSetting("ConnectionStrings:Default", $"Data Source={_db};Default Timeout=30");
            b.UseSetting("Worker:Enabled", "false");
        });
        _client = _app.CreateClient();
    }

    public void Dispose()
    {
        _client.Dispose();
        _app.Dispose();
        SqliteConnection.ClearAllPools();
        try { File.Delete(_db); } catch (IOException) { }
    }

    private async Task<Guid> Invoice(string number = "INV-1")
    {
        var response = await _client.PostAsJsonAsync("/api/invoices", new { number, customer = "Acme", amount = 125.5 });
        response.EnsureSuccessStatusCode();
        return (await response.Content.ReadFromJsonAsync<JsonElement>()).GetProperty("id").GetGuid();
    }

    private Task Mode(string mode) => _client.PostAsJsonAsync("/api/broker/mode", new { mode });
    private Task Run() => _client.PostAsync("/api/worker/run", null);
    private Task Skip(int seconds) => _client.PostAsJsonAsync("/api/demo/skip", new { seconds });
    private async Task<List<JsonElement>> Outbox() => await _client.GetFromJsonAsync<List<JsonElement>>("/api/outbox") ?? [];
    private async Task<List<JsonElement>> Deliveries() => await _client.GetFromJsonAsync<List<JsonElement>>("/api/broker/deliveries") ?? [];

    private async Task<int> EmailsFor(Guid invoice)
    {
        var effects = await _client.GetFromJsonAsync<JsonElement>("/api/effects");
        return effects.GetProperty("emailsSent").EnumerateArray().Where(e => e.GetProperty("invoiceId").GetGuid() == invoice).Select(e => e.GetProperty("count").GetInt32()).FirstOrDefault();
    }

    [Fact]
    public async Task A_new_invoice_always_leaves_a_message_in_the_outbox()
    {
        await Invoice();

        var rows = await Outbox();

        Assert.Equal("InvoiceCreated", Assert.Single(rows).GetProperty("type").GetString());
        Assert.Equal("Pending", rows[0].GetProperty("status").GetString());
    }

    [Fact]
    public async Task A_failed_save_leaves_neither_an_invoice_nor_a_message()
    {
        await Invoice("INV-1");

        var duplicate = await _client.PostAsJsonAsync("/api/invoices", new { number = "INV-1", customer = "Other", amount = 5 });

        Assert.Equal(HttpStatusCode.Conflict, duplicate.StatusCode);
        Assert.Single(await Outbox());
        Assert.Single(await _client.GetFromJsonAsync<List<JsonElement>>("/api/invoices") ?? []);
    }

    [Fact]
    public async Task While_the_broker_is_down_the_message_waits_and_is_published_once_it_is_back()
    {
        var invoice = await Invoice();
        await Mode("Down");

        await Run();
        var retrying = (await Outbox()).Single();
        Assert.Equal("Retrying", retrying.GetProperty("status").GetString());
        Assert.Equal(1, retrying.GetProperty("attempts").GetInt32());
        Assert.Equal(0, await EmailsFor(invoice));

        await Mode("Healthy");
        await Run();
        Assert.Equal("Retrying", (await Outbox()).Single().GetProperty("status").GetString());
        await Skip(10);
        await Run();

        Assert.Equal("Published", (await Outbox()).Single().GetProperty("status").GetString());
        Assert.Equal(1, await EmailsFor(invoice));
    }

    [Fact]
    public async Task A_lost_acknowledgement_causes_a_second_delivery_but_the_effect_happens_once()
    {
        var invoice = await Invoice();
        await Mode("LoseReply");

        await Run();
        await Skip(10);
        await Run();

        var deliveries = await Deliveries();
        Assert.Equal(2, deliveries.Count);
        Assert.False(deliveries[0].GetProperty("duplicate").GetBoolean());
        Assert.True(deliveries[1].GetProperty("duplicate").GetBoolean());
        Assert.True(deliveries[0].GetProperty("effectApplied").GetBoolean());
        Assert.False(deliveries[1].GetProperty("effectApplied").GetBoolean());
        Assert.Equal(1, await EmailsFor(invoice));
        Assert.Equal("Published", (await Outbox()).Single().GetProperty("status").GetString());
    }

    [Fact]
    public async Task A_message_that_keeps_failing_is_dead_lettered_and_can_be_retried()
    {
        var invoice = await Invoice();
        await Mode("Down");

        for (var i = 0; i < 6; i++)
        {
            await Run();
            await Skip(600);
        }

        var dead = (await Outbox()).Single();
        Assert.Equal("DeadLettered", dead.GetProperty("status").GetString());
        Assert.Equal(4, dead.GetProperty("attempts").GetInt32());

        await Mode("Healthy");
        Assert.Equal(HttpStatusCode.Accepted, (await _client.PostAsync($"/api/outbox/{dead.GetProperty("id").GetGuid()}/retry", null)).StatusCode);
        await Run();

        Assert.Equal("Published", (await Outbox()).Single().GetProperty("status").GetString());
        Assert.Equal(1, await EmailsFor(invoice));
    }

    [Fact]
    public async Task Only_dead_lettered_messages_can_be_retried()
    {
        await Invoice();
        var id = (await Outbox()).Single().GetProperty("id").GetGuid();

        Assert.Equal(HttpStatusCode.Conflict, (await _client.PostAsync($"/api/outbox/{id}/retry", null)).StatusCode);
    }

    [Fact]
    public async Task Many_invoices_are_each_emailed_exactly_once()
    {
        var invoices = new List<Guid>();
        for (var i = 0; i < 8; i++)
        {
            invoices.Add(await Invoice($"INV-{i}"));
        }

        await Mode("LoseReply");
        await Run();
        await Skip(10);
        await Run();

        foreach (var invoice in invoices)
        {
            Assert.Equal(1, await EmailsFor(invoice));
        }

        Assert.All(await Outbox(), m => Assert.Equal("Published", m.GetProperty("status").GetString()));
    }
}
