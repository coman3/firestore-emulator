# Lightweight Firestore emulator

A small Go replacement for the Firebase CLI + Java Firestore emulator, for local development.

- **One process for every project and database.** Projects are created on first use, and each database has its own documents.
- **Only current documents are kept**, as compact protobuf bytes. The Java emulator retains every
  version of every document for the life of the process, so its memory grows with write volume.
- **gRPC and REST on one port**, covering the common Firestore operations including gets, batch gets,
  queries (filters including `Or`/`And`, `in`, `not-in`, `array-contains(-any)`, `!=`, ordering,
  cursors, offset/limit, projections, collection groups), `Count`/`Sum`/`Average` aggregations,
  commits with preconditions and field transforms, batch writes, transactions, and listing
  documents and collections. Realtime `Listen`/`Write` streams and `PartitionQuery` are not
  implemented.
- **Cloud Firestore export format** for `POST /emulator/v1/projects/{p}:export` and `:import`,
  byte-compatible with the Java emulator, plus `DELETE /emulator/v1/projects/{p}/databases/{d}/documents`
  to clear a database. Exports can move between the two emulators.
- **Transactions are optimistic**, like the Java emulator's: a commit is aborted (and the client
  retries) if a document the transaction read has changed, or if a query it ran would now return a
  different set of documents.
- **Local tools only.** It listens on loopback, and its REST API refuses requests a web page in the
  browser could send: from a non-local `Origin`, with a non-local `Host` (DNS rebinding), or with a
  body that isn't `application/json` (which would skip the CORS preflight). gRPC is unaffected.

It is more lenient than Firestore in a few ways, so a query can work here and fail in a real
environment: composite indexes aren't required, security rules aren't evaluated, and there are no
realtime listeners.

Measured on the same machine with 23,400 documents across 13 projects, then rewritten 8 times:

| | Idle | After load | After 8 rewrites | Load | 78 queries |
|---|---|---|---|---|---|
| Java emulator (original benchmark JVM settings) | 146 MB | 490 MB | 1,444 MB | 6.3 s | 1.88 s |
| This emulator | 12 MB | 49 MB | 55 MB | 1.8 s | 0.40 s |

## Run it

With Go 1.24 or later, run this from the repository root.

```bash
go run . --host 127.0.0.1 --port 8080
```

In the shell running your application, point your Firestore client at the emulator and
configure a project ID such as `demo-local` in your client.

```bash
export FIRESTORE_EMULATOR_HOST=127.0.0.1:8080
```

Documents live in memory until the process exits. Export any data you want to keep.

`GOGC` defaults to 50; set it to trade memory for CPU.

## Test it

```bash
go test ./...
```

`testdata/java-export` was written by the Java emulator; the tests import it, compare every value
with what that emulator served, and check our exports are byte-identical where Java's ordering
allows.

`conformance/` uses the Firestore .NET client to compare two running emulators. The runner
clears the default databases in the `conformance` and `conformance-2` projects on each run.

```bash
cd conformance
dotnet run -c Release -- 127.0.0.1:8080 > java.txt   # Java emulator
dotnet run -c Release -- 127.0.0.1:8081 > lite.txt   # this emulator
diff java.txt lite.txt
```

The two expected differences are Java emulator limitations, where this emulator returns what
Firestore does: Java rejects descending document-id order ("does not support descending key
scans") and returns `0`/`null` for `Sum`/`Average` requested through the .NET client.
