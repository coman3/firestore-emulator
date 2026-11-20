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

See the [benchmark results](BENCHMARK.md) for query latency, startup, import time, and memory use
across 5,000, 10,000, and 100,000 seeded documents on both emulators.

## Development note

This emulator was developed with AI assistance. When we released v0.1, we had used it internally
for over two weeks without encountering any issues. That experience covers the Firestore features
our applications exercise.

## Run it

Download the archive for your OS and CPU from the [latest release](https://github.com/coman3/firestore-emulator/releases/latest).
Extract it and run `./firestore-emulator` (or `.\firestore-emulator.exe` in Windows PowerShell).
The default address is `127.0.0.1:8080`.

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

## Snapshots for end-to-end tests

Seed a project once through your application or Firestore client. After the seed writes finish,
export the database to a named directory. The emulator reads and writes these paths on its own
filesystem. This example runs the shell and emulator on the same machine.

```bash
admin=http://127.0.0.1:8080/emulator/v1/projects/demo-e2e
mkdir -p "$PWD/snapshots"
curl -fsS "$admin:export" -H 'Content-Type: application/json' \
  -d "{\"export_directory\":\"$PWD/snapshots\",\"export_name\":\"baseline\"}"
```

Keep the whole `snapshots/baseline` directory together. Before each test run, stop any writers,
clear the test database, and import the baseline. Import creates or overwrites the exported
documents. Clearing first also removes documents left behind by earlier tests.

```bash
curl -fsS -X DELETE "$admin/databases/(default)/documents" &&
  curl -fsS "$admin:import" -H 'Content-Type: application/json' \
    -d "{\"export_directory\":\"$PWD/snapshots/baseline\"}"
```

For parallel tests, give each worker its own project ID and import the baseline into that
project. Document paths are restored under the target database. Stored document-reference
values retain their original project, so seed those references for the worker when needed.
For a named database, include `"database": "projects/demo-e2e/databases/my-db"` in both POST
bodies and use `my-db` in the DELETE URL. Each export captures one database. To reproduce a
failed run, export its state under a fresh name before teardown.

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

## Cut a release

Tag the commit you want to ship and push the tag. For example, to release v0.2 from `main`:

```bash
git switch main && git pull --ff-only &&
  git tag -a v0.2 -m "Release v0.2" && git push origin v0.2
```

GitHub Actions tests the tagged commit, builds all six archives, and publishes them on a
GitHub Release named after the tag. Each release includes Windows, Linux, and macOS binaries
for amd64 and arm64.
