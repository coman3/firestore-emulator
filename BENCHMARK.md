# Local Firestore emulator benchmark

Measured [1bbdb3d](https://github.com/coman3/firestore-emulator/commit/1bbdb3dd43eaf6b83e4738ff38044f001782583b) in [Actions run 44643740018](https://github.com/coman3/firestore-emulator/actions/runs/44643740018).

| Emulator | Startup (ms) | Project import (ms) | Idle RSS (MiB) | After import RSS (MiB) |
|---|---:|---:|---:|---:|
| Java | 1,485.8 | 388.3 | 242.6 | 330.9 |
| Go | 11.7 | 18.7 | 14.4 | 24.5 |

## Method

- Median of 3 fresh processes per emulator on one Linux x86_64 runner (ubuntu24). The emulators run sequentially, alternating order between rounds.
- Startup spans process launch through the first successful Firestore collection read (10 ms between failed probes). Downloads, compilation and fixture creation are excluded.
- Both import the same on-disk export into one empty project. It contains 5,000 documents, each with an integer and a 256-byte string. Import time covers the local HTTP import request through its response. A separate count query verifies every trial.
- Resident set size (RSS) is sampled for the emulator process one second after readiness and one second after import, before verification. These are point samples. JVM and Go runtime memory are included. The Java emulator runs directly with default JVM settings.
- Toolchain: `cloud-firestore-emulator-v1.22.0.jar`, `openjdk version "21.0.13" 2026-10-20 LTS`, `go version go1.27.4 linux/amd64`.

Refresh the report with [Run workflow](https://github.com/coman3/firestore-emulator/actions/workflows/benchmark.yml) on `main`.
