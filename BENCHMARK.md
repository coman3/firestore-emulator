# Local Firestore emulator benchmark

Measured [009ecd9](https://github.com/coman3/firestore-emulator/commit/009ecd9118dce5e6c545b693023f49864a2f38ba) in [Actions run 44644329630](https://github.com/coman3/firestore-emulator/actions/runs/44644329630).

## Query latency

| Documents | Emulator | Equality filter (ms) | Range + top 20 (ms) | Count all (ms) |
|---:|---|---:|---:|---:|
| 5,000 | Java | 88.8 | 41.2 | 36.4 |
| 5,000 | Go | 10.0 | 9.7 | 28.7 |
| 10,000 | Java | 111.4 | 83.6 | 86.9 |
| 10,000 | Go | 18.2 | 17.3 | 57.9 |
| 100,000 | Java | 1,446.9 | 1,056.0 | 1,061.6 |
| 100,000 | Go | 177.6 | 171.8 | 740.7 |

## Startup, import and memory

| Documents | Emulator | Startup (ms) | Import (ms) | Idle RSS (MiB) | After import RSS (MiB) |
|---:|---|---:|---:|---:|---:|
| 5,000 | Java | 1,449.8 | 408.7 | 244.0 | 323.9 |
| 5,000 | Go | 11.7 | 22.4 | 14.5 | 26.2 |
| 10,000 | Java | 1,434.6 | 529.7 | 232.6 | 374.8 |
| 10,000 | Go | 11.9 | 41.7 | 15.6 | 34.8 |
| 100,000 | Java | 1,407.4 | 2,301.4 | 243.0 | 1,726.1 |
| 100,000 | Go | 11.7 | 377.2 | 14.6 | 193.6 |

## Method

- Median of 3 fresh processes per emulator on one Linux x86_64 runner (ubuntu24). The emulators run sequentially, alternating order between rounds.
- Startup spans process launch through the first successful Firestore collection read (10 ms between failed probes). Downloads, compilation and fixture creation are excluded.
- At each size both engines import the same export. Seed 42 generates a uniform bucket (0–255), score (0–9,999), and 256-character hex string per document. Larger datasets extend the smaller ones. Import time spans the local HTTP request through its response.
- Query latency includes loopback HTTP and JSON decoding. In each process each query runs once as a warm-up and once for timing. The equality filter returns every document in bucket 42. The range query returns up to 20 documents scoring at least 9,900, highest score first. Count aggregates the entire collection. Both document queries verify ordered IDs against the seed data, and the count must match the fixture size.
- Resident set size (RSS) is sampled for the emulator process one second after readiness and one second after import, before verification. These are point samples. JVM and Go runtime memory are included. The Java emulator runs directly with default JVM settings.
- Toolchain: `cloud-firestore-emulator-v1.22.0.jar`, `openjdk version "21.0.13" 2026-10-20 LTS`, `go version go1.27.4 linux/amd64`.

Refresh the report with [Run workflow](https://github.com/coman3/firestore-emulator/actions/workflows/benchmark.yml) on `main`.
