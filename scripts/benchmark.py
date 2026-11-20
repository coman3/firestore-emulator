"""Compare two local emulator processes against one synthetic Firestore export."""

import json
import os
import platform
import random
import statistics
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

PROJECT = "demo-benchmark"
DATABASE = f"projects/{PROJECT}/databases/(default)"
PORT = 18081
SIZES = (5_000, 10_000, 100_000)
SEED = 42
TRIALS = 3


def request(path, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = Request(f"http://127.0.0.1:{PORT}/{path}", data=data,
                  headers={"Content-Type": "application/json"})
    with urlopen(req, timeout=60) as response:
        return json.load(response)


@contextmanager
def emulator(command):
    with tempfile.TemporaryFile(mode="w+") as log:
        start = time.perf_counter()
        process = subprocess.Popen([*command, "--host", "127.0.0.1", "--port", str(PORT)],
                                   stdout=log, stderr=subprocess.STDOUT)
        try:
            while True:
                if process.poll() is not None or time.perf_counter() - start > 60:
                    raise RuntimeError("Emulator failed to start")
                try:
                    request(f"v1/{DATABASE}/documents/items?pageSize=1")
                    break
                except URLError:
                    time.sleep(0.01)
            yield process.pid, time.perf_counter() - start
        except Exception:
            log.seek(0)
            print(log.read(), file=sys.stderr)
            raise
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


def fixture(directory, size):
    rng = random.Random(SEED)
    bucket_matches = []
    high_scores = []
    for offset in range(0, size, 250):
        writes = []
        for i in range(offset, min(offset + 250, size)):
            bucket = rng.randrange(256)
            score = rng.randrange(10_000)
            name = f"{DATABASE}/documents/items/{i:06d}"
            fields = {"bucket": {"integerValue": str(bucket)}, "score": {"integerValue": str(score)},
                      "text": {"stringValue": rng.randbytes(128).hex()}}
            writes.append({"update": {"name": name, "fields": fields}})
            if bucket == 42:
                bucket_matches.append(name)
            if score >= 9900:
                high_scores.append((score, name))
        request(f"v1/{DATABASE}/documents:commit", {"writes": writes})
    label = f"baseline-{size}"
    request(f"emulator/v1/projects/{PROJECT}:export",
            {"database": DATABASE, "export_directory": directory, "export_name": label})
    top_names = [name for _, name in sorted(high_scores, reverse=True)[:20]]
    return Path(directory, label, f"{label}.overall_export_metadata"), bucket_matches, top_names


def filter_query(field, op, value):
    return {"from": [{"collectionId": "items"}], "where": {"fieldFilter": {
        "field": {"fieldPath": field}, "op": op, "value": {"integerValue": str(value)}}}}


def query_times(size, bucket_matches, top_names):
    root = f"v1/{DATABASE}/documents"
    bucket = filter_query("bucket", "EQUAL", 42)
    top = filter_query("score", "GREATER_THAN_OR_EQUAL", 9900)
    top.update(orderBy=[{"field": {"fieldPath": "score"}, "direction": "DESCENDING"}], limit=20)
    count = {"structuredQuery": {"from": [{"collectionId": "items"}]},
             "aggregations": [{"alias": "total", "count": {}}]}
    times = []
    for method, body, expected in [
        ("runQuery", {"structuredQuery": bucket}, bucket_matches),
        ("runQuery", {"structuredQuery": top}, top_names),
        ("runAggregationQuery", {"structuredAggregationQuery": count}, size),
    ]:
        for _ in range(2):  # Warm once, then time the same request.
            start = time.perf_counter()
            rows = request(f"{root}:{method}", body)
            elapsed = (time.perf_counter() - start) * 1000
            if method == "runQuery":
                actual = [row["document"]["name"] for row in rows if "document" in row]
            else:
                values = [row["result"]["aggregateFields"]["total"] for row in rows if "result" in row]
                actual = int(values[0]["integerValue"]) if len(values) == 1 else None
            if actual != expected:
                raise ValueError(f"Unexpected {method} result for {size:,} documents: {body}")
        times.append(elapsed)
    return times


def rss(pid):
    return int(subprocess.check_output(["ps", "-o", "rss=", "-p", str(pid)])) / 1024


def measure(command, size, snapshot, bucket_matches, top_names):
    with emulator(command) as (pid, startup):
        time.sleep(1)
        empty = rss(pid)
        start = time.perf_counter()
        request(f"emulator/v1/projects/{PROJECT}:import",
                {"database": DATABASE, "export_directory": str(snapshot)})
        elapsed = time.perf_counter() - start
        time.sleep(1)
        loaded = rss(pid)
        return startup * 1000, elapsed * 1000, empty, loaded, *query_times(size, bucket_matches, top_names)


def main():
    binary, jar = sys.argv[1:]
    commands = {"Java": ["java", "-jar", jar], "Go": [binary]}
    samples = {}
    with tempfile.TemporaryDirectory() as directory:
        for size in SIZES:
            with emulator(commands["Go"]):
                data = fixture(directory, size)
            for trial in range(TRIALS):
                for name in (commands if trial % 2 == 0 else reversed(commands)):
                    measurements = measure(commands[name], size, *data)
                    samples.setdefault((size, name), []).append(measurements)
                    print(size, name, measurements, flush=True)

    repo = os.environ["GITHUB_REPOSITORY"]
    sha = os.environ["GITHUB_SHA"]
    run = os.environ["GITHUB_RUN_ID"]
    report = [
        "# Local Firestore emulator benchmark", "",
        f"Measured [{sha[:7]}](https://github.com/{repo}/commit/{sha}) in "
        f"[Actions run {run}](https://github.com/{repo}/actions/runs/{run}).", "",
    ]
    for title, headings, columns in [
        ("Query latency", ["Equality filter (ms)", "Range + top 20 (ms)", "Count all (ms)"], range(4, 7)),
        ("Startup, import and memory", ["Startup (ms)", "Import (ms)", "Idle RSS (MiB)",
                                        "After import RSS (MiB)"], range(4)),
    ]:
        report.extend([f"## {title}", "", "| Documents | Emulator | " + " | ".join(headings) + " |",
                       "|---:|---|" + "---:|" * len(headings)])
        for (size, name), trials in samples.items():
            cells = [f"{statistics.median(row[col] for row in trials):,.1f}" for col in columns]
            report.append("| " + " | ".join([f"{size:,}", name, *cells]) + " |")
        report.append("")

    java = subprocess.check_output(["java", "-version"], stderr=subprocess.STDOUT, text=True).splitlines()[0]
    go = subprocess.check_output(["go", "version"], text=True).strip()
    report.extend(["## Method", "",
        f"- Median of {TRIALS} fresh processes per emulator on one {platform.system()} "
        f"{platform.machine()} runner ({os.environ.get('ImageOS', platform.release())}). "
        "The emulators run sequentially, alternating order between rounds.",
        "- Startup spans process launch through the first successful Firestore collection read "
        "(10 ms between failed probes). Downloads, compilation and fixture creation are excluded.",
        f"- At each size both engines import the same export. Seed {SEED} generates a uniform bucket "
        "(0–255), score (0–9,999), and 256-character hex string per document. Larger datasets "
        "extend the smaller ones. Import time spans the local HTTP request through its response.",
        "- Query latency includes loopback HTTP and JSON decoding. In each process each query runs "
        "once as a warm-up and once for timing. The equality filter returns every document in bucket 42. "
        "The range query returns up to 20 documents scoring at least 9,900, highest score first. "
        "Count aggregates the entire collection. Both document queries verify ordered IDs against the "
        "seed data, and the count must match the fixture size.",
        "- Resident set size (RSS) is sampled for the emulator process one second after readiness "
        "and one second after import, before verification. These are point samples. JVM and Go "
        "runtime memory are included. The Java emulator runs directly with default JVM settings.",
        f"- Toolchain: `{Path(jar).name}`, `{java}`, `{go}`.", "",
        f"Refresh the report with [Run workflow](https://github.com/{repo}/actions/workflows/benchmark.yml) on `main`.", ""])
    Path("BENCHMARK.md").write_text("\n".join(report))


if __name__ == "__main__":
    main()
