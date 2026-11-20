"""Compare two local emulator processes against one synthetic Firestore export."""

import json
import os
import platform
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
DOCUMENTS = 5000
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


def fixture(directory):
    for offset in range(0, DOCUMENTS, 250):
        writes = []
        for i in range(offset, offset + 250):
            fields = {"sequence": {"integerValue": str(i)}, "text": {"stringValue": "x" * 256}}
            writes.append({"update": {"name": f"{DATABASE}/documents/items/{i}", "fields": fields}})
        request(f"v1/{DATABASE}/documents:commit", {"writes": writes})
    request(f"emulator/v1/projects/{PROJECT}:export",
            {"database": DATABASE, "export_directory": directory, "export_name": "baseline"})
    return Path(directory, "baseline", "baseline.overall_export_metadata")


def rss(pid):
    return int(subprocess.check_output(["ps", "-o", "rss=", "-p", str(pid)])) / 1024


def measure(command, snapshot):
    with emulator(command) as (pid, startup):
        time.sleep(1)
        empty = rss(pid)
        start = time.perf_counter()
        request(f"emulator/v1/projects/{PROJECT}:import",
                {"database": DATABASE, "export_directory": str(snapshot)})
        elapsed = time.perf_counter() - start
        time.sleep(1)
        loaded = rss(pid)
        query = {"structuredAggregationQuery": {"structuredQuery": {"from": [{"collectionId": "items"}]},
                 "aggregations": [{"alias": "total", "count": {}}]}}
        results = request(f"v1/{DATABASE}/documents:runAggregationQuery", query)
        counts = [r["result"]["aggregateFields"]["total"]["integerValue"] for r in results if "result" in r]
        if counts != [str(DOCUMENTS)]:
            raise ValueError(f"Expected {DOCUMENTS} imported documents, got {counts}")
        return startup * 1000, elapsed * 1000, empty, loaded


def main():
    binary, jar = sys.argv[1:]
    commands = {"Java": ["java", "-jar", jar], "Go": [binary]}
    samples = {name: [] for name in commands}
    with tempfile.TemporaryDirectory() as directory:
        with emulator(commands["Go"]):
            snapshot = fixture(directory)
        for trial in range(TRIALS):
            for name in (commands if trial % 2 == 0 else reversed(commands)):
                samples[name].append(measure(commands[name], snapshot))
                print(name, samples[name][-1], flush=True)

    repo = os.environ["GITHUB_REPOSITORY"]
    sha = os.environ["GITHUB_SHA"]
    run = os.environ["GITHUB_RUN_ID"]
    report = [
        "# Local Firestore emulator benchmark", "",
        f"Measured [{sha[:7]}](https://github.com/{repo}/commit/{sha}) in "
        f"[Actions run {run}](https://github.com/{repo}/actions/runs/{run}).", "",
        "| Emulator | Startup (ms) | Project import (ms) | Idle RSS (MiB) | After import RSS (MiB) |",
        "|---|---:|---:|---:|---:|",
    ]
    for name, values in samples.items():
        cells = [f"{statistics.median(column):,.1f}" for column in zip(*values)]
        report.append("| " + " | ".join([name, *cells]) + " |")

    java = subprocess.check_output(["java", "-version"], stderr=subprocess.STDOUT, text=True).splitlines()[0]
    go = subprocess.check_output(["go", "version"], text=True).strip()
    report.extend(["", "## Method", "",
        f"- Median of {TRIALS} fresh processes per emulator on one {platform.system()} "
        f"{platform.machine()} runner ({os.environ.get('ImageOS', platform.release())}). "
        "The emulators run sequentially, alternating order between rounds.",
        "- Startup spans process launch through the first successful Firestore collection read "
        "(10 ms between failed probes). Downloads, compilation and fixture creation are excluded.",
        f"- Both import the same on-disk export into one empty project. It contains {DOCUMENTS:,} "
        "documents, each with an integer and a 256-byte string. Import time covers the local HTTP "
        "import request through its response. A separate count query verifies every trial.",
        "- Resident set size (RSS) is sampled for the emulator process one second after readiness "
        "and one second after import, before verification. These are point samples. JVM and Go "
        "runtime memory are included. The Java emulator runs directly with default JVM settings.",
        f"- Toolchain: `{Path(jar).name}`, `{java}`, `{go}`.", "",
        f"Refresh the report with [Run workflow](https://github.com/{repo}/actions/workflows/benchmark.yml) on `main`.", ""])
    Path("BENCHMARK.md").write_text("\n".join(report))


if __name__ == "__main__":
    main()
