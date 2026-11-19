// Replays Firestore operations against one emulator and prints a canonical,
// timestamp-free transcript. Run it against two emulators and diff the transcripts:
//
//   dotnet run -- 127.0.0.1:8080 > java.txt
//   dotnet run -- 127.0.0.1:8081 > lite.txt
//   diff java.txt lite.txt
using System.Globalization;
using System.Text;
using Google.Api.Gax;
using Google.Cloud.Firestore;
using Grpc.Core;

var host = args.Length > 0 ? args[0] : "127.0.0.1:8080";
Environment.SetEnvironmentVariable("FIRESTORE_EMULATOR_HOST", host);
var project = "conformance";
var db = await new FirestoreDbBuilder { ProjectId = project, EmulatorDetection = EmulatorDetection.EmulatorOnly }.BuildAsync();
var other = await new FirestoreDbBuilder { ProjectId = project + "-2", EmulatorDetection = EmulatorDetection.EmulatorOnly }.BuildAsync();
using (var http = new HttpClient())
{
    foreach (var p in new[] { project, project + "-2" })
        await http.DeleteAsync($"http://{host}/emulator/v1/projects/{p}/databases/(default)/documents");
}

var output = new StringBuilder();
void Log(string label, object? value) => output.AppendLine($"{label}: {Render(value)}");

async Task Step(string label, Func<Task<object?>> action)
{
    try { Log(label, await action()); }
    catch (RpcException e) { Log(label, $"<rpc {e.StatusCode}>"); }
    catch (Exception e) when (e.InnerException is RpcException inner) { Log(label, $"<rpc {inner.StatusCode}>"); }
    catch (Exception e) { Log(label, $"<{e.GetType().Name}>"); }
}

string Render(object? value) => value switch
{
    null => "null",
    string s => "\"" + s + "\"",
    bool b => b ? "true" : "false",
    long l => l.ToString(CultureInfo.InvariantCulture) + "L",
    int i => i.ToString(CultureInfo.InvariantCulture) + "L",
    double d => double.IsNaN(d) ? "NaN" : d.ToString("R", CultureInfo.InvariantCulture) + "D",
    Timestamp t => "ts(" + t.ToDateTime().ToString("O", CultureInfo.InvariantCulture) + ")",
    Blob b => "bytes(" + Convert.ToBase64String(b.ByteString.ToByteArray()) + ")",
    GeoPoint g => $"geo({g.Latitude.ToString(CultureInfo.InvariantCulture)},{g.Longitude.ToString(CultureInfo.InvariantCulture)})",
    DocumentReference r => "ref(" + r.Path + ")",
    DocumentSnapshot s => s.Exists ? s.Id + Render(s.ToDictionary()) : s.Id + "<missing>",
    IDictionary<string, object> m => "{" + string.Join(",", m.OrderBy(kv => kv.Key, StringComparer.Ordinal).Select(kv => kv.Key + ":" + Render(kv.Value))) + "}",
    System.Collections.IEnumerable e => "[" + string.Join(",", e.Cast<object?>().Select(Render)) + "]",
    _ => value.ToString() ?? "?",
};

static List<string> Ids(QuerySnapshot q) => q.Documents.Select(d => d.Id).ToList();

var fixedTime = Timestamp.FromDateTime(new DateTime(2024, 2, 29, 13, 14, 15, DateTimeKind.Utc).AddTicks(1234567));

// ---- documents and preconditions ----
var people = db.Collection("people");
await Step("create", async () => { await people.Document("ann").CreateAsync(new Dictionary<string, object> { ["name"] = "Ann", ["age"] = 30L }); return "ok"; });
await Step("create existing", async () => { await people.Document("ann").CreateAsync(new Dictionary<string, object> { ["name"] = "Dup" }); return "ok"; });
await Step("update missing", async () => { await people.Document("nobody").UpdateAsync("age", 1L); return "ok"; });
await Step("delete missing", async () => { await people.Document("nobody").DeleteAsync(); return "ok"; });
await Step("delete with exists precondition", async () => { await people.Document("nobody").DeleteAsync(Precondition.MustExist); return "ok"; });
await Step("get", async () => await people.Document("ann").GetSnapshotAsync());
await Step("get missing", async () => await people.Document("nobody").GetSnapshotAsync());
await Step("update nested path", async () =>
{
    await people.Document("ann").UpdateAsync(new Dictionary<string, object> { ["address.city"] = "Melbourne", ["address.postcode"] = "3000", ["tags"] = new[] { "a", "b" } });
    return await people.Document("ann").GetSnapshotAsync();
});
await Step("update field path with dot in name", async () =>
{
    await people.Document("ann").UpdateAsync(new Dictionary<FieldPath, object> { [new FieldPath("weird.key")] = 1L });
    return await people.Document("ann").GetSnapshotAsync();
});
await Step("update delete sentinel", async () =>
{
    await people.Document("ann").UpdateAsync(new Dictionary<string, object> { ["address.postcode"] = FieldValue.Delete, ["age"] = FieldValue.Delete });
    return await people.Document("ann").GetSnapshotAsync();
});
await Step("set replaces", async () =>
{
    await people.Document("ann").SetAsync(new Dictionary<string, object> { ["name"] = "Ann B" });
    return await people.Document("ann").GetSnapshotAsync();
});
await Step("set merge all", async () =>
{
    await people.Document("ann").SetAsync(new Dictionary<string, object> { ["nick"] = "annie", ["nested"] = new Dictionary<string, object> { ["x"] = 1L } }, SetOptions.MergeAll);
    await people.Document("ann").SetAsync(new Dictionary<string, object> { ["nested"] = new Dictionary<string, object> { ["y"] = 2L } }, SetOptions.MergeAll);
    return await people.Document("ann").GetSnapshotAsync();
});
await Step("set merge fields", async () =>
{
    await people.Document("ann").SetAsync(new Dictionary<string, object> { ["nick"] = "an", ["name"] = "ignored" }, SetOptions.MergeFields("nick"));
    return await people.Document("ann").GetSnapshotAsync();
});
await Step("update time unchanged by no-op write", async () =>
{
    var before = (await people.Document("ann").GetSnapshotAsync()).UpdateTime;
    await people.Document("ann").SetAsync(new Dictionary<string, object> { ["nick"] = "an" }, SetOptions.MergeAll);
    var after = (await people.Document("ann").GetSnapshotAsync()).UpdateTime;
    return before == after;
});
await Step("update time precondition", async () =>
{
    var snap = await people.Document("ann").GetSnapshotAsync();
    await people.Document("ann").UpdateAsync("nick", "x", Precondition.LastUpdated(snap.UpdateTime!.Value));
    return "ok";
});
await Step("stale update time precondition", async () =>
{
    await people.Document("ann").UpdateAsync("nick", "y", Precondition.LastUpdated(Timestamp.FromDateTime(new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc))));
    return "ok";
});

// ---- every value type ----
var all = new Dictionary<string, object?>
{
    ["null"] = null, ["true"] = true, ["int"] = -42L, ["big"] = 9007199254740993L, ["double"] = 3.25, ["nan"] = double.NaN,
    ["negInf"] = double.NegativeInfinity, ["ts"] = fixedTime, ["string"] = "héllo ✓", ["empty"] = "",
    ["bytes"] = Blob.CopyFrom(new byte[] { 0, 1, 255 }), ["ref"] = people.Document("ann"), ["geo"] = new GeoPoint(-37.8, 144.9),
    ["array"] = new object?[] { 1L, "x", new Dictionary<string, object> { ["k"] = true }, null }, ["emptyArray"] = Array.Empty<object>(),
    ["emptyMap"] = new Dictionary<string, object>(), ["long"] = new string('z', 3000),
    ["map"] = new Dictionary<string, object> { ["a"] = 1L, ["nested"] = new Dictionary<string, object> { ["deep"] = new[] { 1.5 } } },
};
await Step("all types round trip", async () => { await db.Document("types/all").SetAsync(all); return await db.Document("types/all").GetSnapshotAsync(); });
await Step("server timestamp is a timestamp", async () =>
{
    await db.Document("types/server").SetAsync(new Dictionary<string, object> { ["at"] = FieldValue.ServerTimestamp });
    return (await db.Document("types/server").GetSnapshotAsync()).GetValue<object>("at") is Timestamp;
});

// ---- transforms ----
var counters = db.Collection("counters");
await Step("increment transforms", async () =>
{
    await counters.Document("c").SetAsync(new Dictionary<string, object> { ["i"] = 1L, ["d"] = 1.5, ["s"] = "text" });
    await counters.Document("c").UpdateAsync(new Dictionary<string, object>
    {
        ["i"] = FieldValue.Increment(2L), ["d"] = FieldValue.Increment(1L), ["s"] = FieldValue.Increment(5L),
        ["missing"] = FieldValue.Increment(3.5), ["mixed"] = FieldValue.Increment(1L),
    });
    await counters.Document("c").UpdateAsync("mixed", FieldValue.Increment(0.5));
    return await counters.Document("c").GetSnapshotAsync();
});
await Step("increment saturates", async () =>
{
    await counters.Document("max").SetAsync(new Dictionary<string, object> { ["v"] = long.MaxValue - 1 });
    await counters.Document("max").UpdateAsync("v", FieldValue.Increment(10L));
    return await counters.Document("max").GetSnapshotAsync();
});
await Step("array union and remove", async () =>
{
    await counters.Document("arr").SetAsync(new Dictionary<string, object> { ["a"] = new object[] { 1L, 2L, "x", 2L } });
    await counters.Document("arr").UpdateAsync("a", FieldValue.ArrayUnion(2L, 3L, 1.0, "y"));
    await counters.Document("arr").UpdateAsync("b", FieldValue.ArrayUnion("new"));
    await counters.Document("arr").UpdateAsync("a", FieldValue.ArrayRemove(2L, "x", 9L));
    await counters.Document("arr").UpdateAsync("c", FieldValue.ArrayRemove(1L));
    return await counters.Document("arr").GetSnapshotAsync();
});

// ---- batches ----
await Step("batch", async () =>
{
    var batch = db.StartBatch();
    batch.Create(db.Document("batch/a"), new Dictionary<string, object> { ["n"] = 1L });
    batch.Set(db.Document("batch/b"), new Dictionary<string, object> { ["n"] = 2L });
    batch.Update(db.Document("batch/b"), "n", 3L);
    batch.Delete(db.Document("batch/zzz"));
    var results = await batch.CommitAsync();
    return new object[] { results.Count, await db.Document("batch/a").GetSnapshotAsync(), await db.Document("batch/b").GetSnapshotAsync() };
});
await Step("failed batch is atomic", async () =>
{
    var batch = db.StartBatch();
    batch.Set(db.Document("batch/c"), new Dictionary<string, object> { ["n"] = 1L });
    batch.Create(db.Document("batch/a"), new Dictionary<string, object> { ["n"] = 9L });
    try { await batch.CommitAsync(); } catch (RpcException e) { Log("failed batch error", e.StatusCode.ToString()); }
    return await db.Document("batch/c").GetSnapshotAsync();
});
await Step("get all", async () => (await db.GetAllSnapshotsAsync(new[] { db.Document("batch/a"), db.Document("batch/missing"), db.Document("batch/b") }))
    .OrderBy(s => s.Id, StringComparer.Ordinal).ToList());

// ---- queries ----
var items = db.Collection("items");
var seed = new (string Id, Dictionary<string, object?> Fields)[]
{
    ("i01", new() { ["v"] = 1L, ["g"] = "a", ["tags"] = new[] { "red", "blue" }, ["n"] = 5L }),
    ("i02", new() { ["v"] = 1.0, ["g"] = "a", ["tags"] = new[] { "blue" }, ["n"] = 3L }),
    ("i03", new() { ["v"] = 2.5, ["g"] = "b", ["tags"] = Array.Empty<string>(), ["n"] = 9L }),
    ("i04", new() { ["v"] = double.NaN, ["g"] = "b", ["n"] = 1L }),
    ("i05", new() { ["v"] = null, ["g"] = "c", ["n"] = 7L }),
    ("i06", new() { ["v"] = "1", ["g"] = "c", ["n"] = 2L }),
    ("i07", new() { ["v"] = true, ["g"] = "a" }),
    ("i08", new() { ["v"] = fixedTime, ["g"] = "b", ["n"] = 4L }),
    ("i09", new() { ["v"] = new[] { 1L, 2L }, ["g"] = "a", ["n"] = 6L }),
    ("i10", new() { ["v"] = new Dictionary<string, object> { ["k"] = 1L }, ["g"] = "c", ["n"] = 8L }),
    ("i11", new() { ["g"] = "a", ["n"] = 10L, ["tags"] = new[] { "green" } }),
    ("i12", new() { ["v"] = -3L, ["g"] = "b", ["n"] = 11L, ["score"] = 2.5 }),
    ("i13", new() { ["v"] = 100L, ["g"] = "c", ["n"] = 12L, ["score"] = 1L }),
    ("i14", new() { ["v"] = 1L, ["g"] = "b", ["n"] = 13L, ["tags"] = new[] { "red" } }),
};
await Step("seed items", async () =>
{
    var batch = db.StartBatch();
    foreach (var (id, fields) in seed) batch.Set(items.Document(id), fields);
    await batch.CommitAsync();
    return seed.Length;
});
async Task Q(string label, Query query) => await Step(label, async () => Ids(await query.GetSnapshotAsync()));
await Q("eq int matches double", items.WhereEqualTo("v", 1L));
await Q("eq double", items.WhereEqualTo("v", 1.0));
await Q("eq null", items.WhereEqualTo("v", null));
await Q("eq nan", items.WhereEqualTo("v", double.NaN));
await Q("eq string", items.WhereEqualTo("v", "1"));
await Q("eq array", items.WhereEqualTo("v", new[] { 1L, 2L }));
await Q("eq map", items.WhereEqualTo("v", new Dictionary<string, object> { ["k"] = 1L }));
await Q("neq", items.WhereNotEqualTo("v", 1L));
await Q("neq null", items.WhereNotEqualTo("v", null));
await Q("gt number", items.WhereGreaterThan("v", 1L));
await Q("gte number", items.WhereGreaterThanOrEqualTo("v", 1L));
await Q("lt number", items.WhereLessThan("v", 2L));
await Q("lte string", items.WhereLessThanOrEqualTo("v", "z"));
await Q("gt timestamp", items.WhereGreaterThan("v", Timestamp.FromDateTime(new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc))));
await Q("in", items.WhereIn("v", new object[] { 1L, "1", 100L }));
await Q("in with null", items.WhereIn("v", new object?[] { null, 2.5 }));
await Q("not in", items.WhereNotIn("v", new object[] { 1L, 2.5 }));
await Q("array contains", items.WhereArrayContains("tags", "red"));
await Q("array contains any", items.WhereArrayContainsAny("tags", new[] { "green", "blue" }));
await Q("array contains on non-array", items.WhereArrayContains("g", "a"));
await Q("compound eq", items.WhereEqualTo("g", "a").WhereEqualTo("v", 1L));
await Q("eq plus range", items.WhereEqualTo("g", "b").WhereGreaterThan("n", 3L));
await Q("range with order", items.WhereGreaterThan("n", 3L).OrderBy("n"));
await Q("range desc implicit name order", items.WhereGreaterThan("n", 3L).OrderByDescending("n"));
await Q("multiple inequalities", items.WhereGreaterThan("n", 2L).WhereLessThan("v", 50L));
await Q("order mixed types asc", items.OrderBy("v"));
await Q("order mixed types desc", items.OrderByDescending("v"));
await Q("order by two fields", items.OrderBy("g").OrderByDescending("n"));
await Q("order excludes missing", items.OrderBy("score"));
await Q("limit", items.OrderBy("n").Limit(3));
await Q("offset", items.OrderBy("n").Offset(2).Limit(3));
await Q("limit to last", items.OrderBy("n").LimitToLast(2));
await Q("start after values", items.OrderBy("n").StartAfter(5L));
await Q("start at values", items.OrderBy("n").StartAt(5L));
await Q("end before", items.OrderBy("n").EndBefore(4L));
await Q("end at", items.OrderBy("n").EndAt(4L));
await Step("start after snapshot", async () =>
{
    var cursor = await items.Document("i03").GetSnapshotAsync();
    return Ids(await items.OrderBy("g").StartAfter(cursor).GetSnapshotAsync());
});
await Step("paging by start after", async () =>
{
    var pages = new List<List<string>>();
    Query page = items.OrderBy("n").Limit(4);
    while (true)
    {
        var snap = await page.GetSnapshotAsync();
        if (snap.Count == 0) break;
        pages.Add(Ids(snap));
        page = items.OrderBy("n").StartAfter(snap.Documents[^1]).Limit(4);
    }
    return pages;
});
await Q("or filter", items.Where(Filter.Or(Filter.EqualTo("g", "c"), Filter.GreaterThan("n", 10L))));
await Q("and of ors", items.Where(Filter.And(Filter.Or(Filter.EqualTo("g", "a"), Filter.EqualTo("g", "b")), Filter.LessThan("n", 5L))));
await Q("or with in", items.Where(Filter.Or(Filter.InArray("g", new[] { "c" }), Filter.ArrayContains("tags", "red"))));
await Q("document id eq", items.WhereEqualTo(FieldPath.DocumentId, "i05"));
await Q("document id in", items.WhereIn(FieldPath.DocumentId, new[] { "i02", "i09", "nope" }));
await Q("document id range", items.WhereGreaterThan(FieldPath.DocumentId, "i10"));
await Q("document id ref range", items.WhereGreaterThanOrEqualTo(FieldPath.DocumentId, items.Document("i12")));
await Q("order by document id desc", items.OrderByDescending(FieldPath.DocumentId).Limit(3));
await Step("select", async () => (await items.Select("g", "missing").WhereEqualTo("n", 5L).GetSnapshotAsync()).Documents.Select(d => (object)d).ToList());
await Step("select document id only", async () => (await items.Select(FieldPath.DocumentId).Limit(2).GetSnapshotAsync()).Documents.Select(d => (object)d).ToList());
await Step("count", async () => (await items.Count().GetSnapshotAsync()).Count);
await Step("count filtered", async () => (await items.WhereEqualTo("g", "a").Count().GetSnapshotAsync()).Count);
await Step("count empty", async () => (await items.WhereEqualTo("g", "none").Count().GetSnapshotAsync()).Count);
await Step("sum and average", async () =>
{
    var snap = await items.Aggregate(AggregateField.Sum("n", "s"), AggregateField.Average("n", "a"), AggregateField.Sum("score", "ss"), AggregateField.Average("missing", "am")).GetSnapshotAsync();
    return new object?[] { snap.GetValue<object>("s"), snap.GetValue<object>("a"), snap.GetValue<object>("ss"), snap.GetValue<object>("am") };
});
await Step("stream", async () => { var ids = new List<string>(); await foreach (var d in items.WhereEqualTo("g", "c").StreamAsync()) ids.Add(d.Id); return ids.OrderBy(x => x, StringComparer.Ordinal).ToList(); });

// ---- hierarchy ----
await Step("subcollections", async () =>
{
    await db.Document("orgs/o1/staff/s1").SetAsync(new Dictionary<string, object> { ["role"] = "nurse", ["level"] = 2L });
    await db.Document("orgs/o1/staff/s2").SetAsync(new Dictionary<string, object> { ["role"] = "admin", ["level"] = 1L });
    await db.Document("orgs/ghost/staff/s3").SetAsync(new Dictionary<string, object> { ["role"] = "nurse", ["level"] = 3L });
    await db.Document("orgs/o1").SetAsync(new Dictionary<string, object> { ["name"] = "Org 1" });
    await db.Document("staff/top").SetAsync(new Dictionary<string, object> { ["role"] = "nurse", ["level"] = 9L });
    return Ids(await db.Collection("orgs/o1/staff").GetSnapshotAsync());
});
await Q("collection group", db.CollectionGroup("staff").WhereEqualTo("role", "nurse").OrderBy("level"));
await Q("collection group order by id", db.CollectionGroup("staff").OrderBy(FieldPath.DocumentId));
await Step("list documents with missing parents", async () =>
{
    var ids = new List<string>();
    await foreach (var d in db.Collection("orgs").ListDocumentsAsync()) ids.Add(d.Id);
    return ids;
});
await Step("list root collections", async () => { var ids = new List<string>(); await foreach (var c in db.ListRootCollectionsAsync()) ids.Add(c.Id); return ids; });
await Step("list subcollections of missing parent", async () => { var ids = new List<string>(); await foreach (var c in db.Document("orgs/ghost").ListCollectionsAsync()) ids.Add(c.Id); return ids; });
await Step("projects are isolated", async () =>
{
    await other.Document("people/ann").SetAsync(new Dictionary<string, object> { ["name"] = "Other Ann" });
    return new object[] { await other.Document("people/ann").GetSnapshotAsync(), await people.Document("ann").GetSnapshotAsync() };
});

// ---- transactions ----
await Step("transaction read modify write", async () =>
{
    await db.Document("tx/t").SetAsync(new Dictionary<string, object> { ["n"] = 1L });
    var result = await db.RunTransactionAsync(async tx =>
    {
        var snap = await tx.GetSnapshotAsync(db.Document("tx/t"));
        tx.Update(db.Document("tx/t"), "n", snap.GetValue<long>("n") + 10);
        return snap.GetValue<long>("n");
    });
    return new object[] { result, await db.Document("tx/t").GetSnapshotAsync() };
});
await Step("transaction with query", async () => await db.RunTransactionAsync(async tx =>
{
    var snap = await tx.GetSnapshotAsync(items.WhereEqualTo("g", "c"));
    tx.Set(db.Document("tx/summary"), new Dictionary<string, object> { ["count"] = (long)snap.Count });
    return snap.Count;
}));
await Step("concurrent transactions serialise", async () =>
{
    await db.Document("tx/counter").SetAsync(new Dictionary<string, object> { ["n"] = 0L });
    async Task Bump()
    {
        for (var i = 0; i < 15; i++)
            await db.RunTransactionAsync(async tx =>
            {
                var snap = await tx.GetSnapshotAsync(db.Document("tx/counter"));
                tx.Update(db.Document("tx/counter"), "n", snap.GetValue<long>("n") + 1);
            }, TransactionOptions.ForMaxAttempts(50));
    }
    await Task.WhenAll(Bump(), Bump(), Bump());
    return await db.Document("tx/counter").GetSnapshotAsync();
});
await Step("transaction failure rolls back", async () =>
{
    try
    {
        await db.RunTransactionAsync(async tx =>
        {
            await tx.GetSnapshotAsync(db.Document("tx/t"));
            tx.Set(db.Document("tx/never"), new Dictionary<string, object> { ["x"] = 1L });
            throw new InvalidOperationException("boom");
        });
    }
    catch (InvalidOperationException) { }
    return await db.Document("tx/never").GetSnapshotAsync();
});
await Step("create inside transaction on existing", async () =>
{
    await db.RunTransactionAsync(tx => { tx.Create(db.Document("tx/t"), new Dictionary<string, object> { ["x"] = 1L }); return Task.CompletedTask; },
        TransactionOptions.ForMaxAttempts(1));
    return "ok";
});

// ---- ids ----
await Step("unusual ids", async () =>
{
    foreach (var id in new[] { "with space", "üñí©ødé", "a.b", "__id7__x", "-dash" })
        await db.Collection("ids").Document(id).SetAsync(new Dictionary<string, object> { ["id"] = id });
    return Ids(await db.Collection("ids").OrderBy(FieldPath.DocumentId).GetSnapshotAsync());
});
await Step("auto id length", async () => (await db.Collection("auto").AddAsync(new Dictionary<string, object> { ["x"] = 1L })).Id.Length);

Console.Write(output.ToString());
