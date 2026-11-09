# Test entry points

Maven commands from the repository root build all three runtime modules. Add
`-pl flink/1.16 -am` (or `flink/1.17`, `flink/1.20`) to select one. The Emulator
runner and readiness reporter accept `--flink-line` and default to `1.20`.
Reports below are relative to the selected module's `flink/<line>/` directory.

| Suite | Entry | Coverage and prerequisites |
| --- | --- | --- |
| Emulator | `python3 tools/run-emulator-tests.py` | Disposable Docker service, dummy credentials; reusable SQL journeys and parameter matrices for types, Catalog, Source, write modes, concurrent writes and fault recovery. Fails on unavailable Docker or protocol mismatch; no cloud identity. |
| Offline | `mvn clean verify` | Latest-partition discovery/read/state/options, shuffle assignment/coordinator/recovery, real MiniCluster fault injection, restored Catalog/Table factory validation and schema/type conversion, environment guards. No service identity needed. |
| Distributed jar | `mvn -Ppublic,release-artifacts clean verify` | Uses the shaded jar plus provided Flink dependencies in an isolated class loader. Checks Factory, Session, Jackson Module/JsonFactory/ObjectCodec SPI, XML transport with JDK StAX, quoted SQL CSV readback, legal files and the exact bundled dependency closure. |
| Latest partition E2E | `mvn -Pmaxcompute-integration -Dit.test=LatestPartitionE2EITCase -Dmaxcompute.e2e.allow-write=true verify` | PK Delta and PK DLMV, latest-partition switch, ignored writes to old partitions, checkpoint commit and injected restart. Needs a schema-capable test project with CDC/DLMV support. |
| Dynamic shuffle E2E | `mvn -Pmaxcompute-integration -Dit.test=DynamicShuffleSinkRecoverySystemTest -Dmaxcompute.e2e.allow-write=true verify` | 20,000 PK rows over four partitions, injected restart, SQL no-loss/idempotency readback, 5,000 latest-value rewrites and resource cleanup. |

Use JDK 17. Inject `ODPS_ACCESS_ID`, `ODPS_ACCESS_KEY`, `MAXCOMPUTE_PROJECT` and
`MAXCOMPUTE_ENDPOINT` (or `ODPS_ENDPOINT`); `ODPS_SECURITY_TOKEN` is supported by the latest-partition Source suite. FM14
currently requires AK/SK: its SQL table fixture does not propagate STS tokens and
rejects them before creating a table.
Read approved identities in place or use CI secret references. Do not put credentials in
source configuration, JVM command-line properties, archived logs or checkpoint evidence.

The explicit service suites fail on missing opt-in/identity/environment capabilities;
they do not report a skip as success. UUID object names, `lifecycle 1`, job cancellation,
drop/absence checks and deletion of credential-bearing checkpoint files are part of
acceptance. A service failure needs its own cleanup evidence. FM14 bounds each job wait, cancels
unfinished jobs, waits for termination, and checks an isolated temporary directory;
files created by concurrent processes are outside that assertion.

Legacy fixed-table tests are marked `MaxComputeIntegration` and excluded from the
default suite; legacy `*SystemTest` cases are also excluded. They are not included in
the public export or the release acceptance profile. Old commented tests use removed
interfaces and are not counted as coverage. Current Catalog/Table factory and schema
contracts have executable offline replacements. The current lookup provider is
unimplemented and rejects lookup joins. Broad Catalog CRUD, ordinary/batch
Source and all Sink mode/type combinations need their own real-service
matrix before claiming coverage for those configurations.

Passing offline/SPI tests does not prove service acceptance or a published artifact.

## Emulator regression

The Storage v1 suite defaults to the released Emulator `1.2.0`, pinned by digest in
[tools/emulator-image.json](../tools/emulator-image.json). The image is Linux AMD64;
Apple Silicon uses Docker platform emulation. For offline use, download the Release
archive and its SHA-256 file, verify the archive, then `docker load -i <archive.tar.gz>`.
The runner reuses a loaded image only when its content-addressed image ID matches
the published image lock; a version tag alone cannot satisfy this check.
Use `--image` and `--platform` only
when testing another source candidate. The runner binds an
OS-selected port only on 127.0.0.1, waits for readiness, passes both SDK endpoints
through connector options, and removes only its own container in a finally block.
Each test uses a UUID table and asserts its absence after cleanup. Missing Docker,
unsupported operations and unavailable endpoints fail the suite instead of skipping.
CI uses the same published digest unless explicitly overridden.

Run the default published image, or a locally built ARM64 candidate:

```sh
python3 tools/run-emulator-tests.py
python3 tools/run-emulator-tests.py --image <reviewed-image> --platform linux/arm64
```

The emulator is suitable for high-volume routine metadata, SQL fixtures, Tunnel and
Storage v1 batch read and Arrow regression. CDC/incremental reads, DLMV, authentication/permissions,
production quota/throughput and distributed service recovery remain real-service
acceptance gates. Passing the emulator matrix does not establish these guarantees.

## Protocol conformance before connector acceptance

Keep emulator wire-contract tests independent from the connector assertions. Compare
routes, request validation, response schemas, record ranges, nulls, timestamp precision
and IPC compression against the service implementation and public SDK. A connector
round trip alone cannot establish equivalence: matching producer/consumer mistakes can
cancel each other out.

The candidate checks four Arrow units (second, milli, micro, nano), nested timestamps,
uncompressed/ZSTD/LZ4_FRAME IPC streams, batch boundaries and nulls. Storage v1 tests
check case-insensitive Arrow/V5 negotiation, row-offset boundaries, resume within the
requested range and malformed parameters. Connector suites separately exercise Catalog
CRUD and cache refresh, Source projection, and a wide-table SQL pipeline with all type
variants, nulls and empty data. Memory/file caches and row-offset/size reads are exercised
in the same pipeline.

Physical multi-split planning, bucket splits, incremental reads and distributed storage
behavior remain unverified. Unsupported planning requests fail explicitly. These cases
must acquire independent conformance evidence before their emulator results count as
connector acceptance. Do not relax service semantics to make a connector test pass.

Run `--clean` for reported coverage. Only a successful complete run is an acceptance
result; incremental JaCoCo files from debugging are not the reported baseline. The
`coverage-check` profile enforces 80% production line coverage without package exclusions.

## Flink SQL scenario matrix

`EmulatorSqlRoundTripITCase` is one E2E template carrying all 17 type variants in a
single wide table. It executes `INSERT VALUES`, `INSERT SELECT` (all rows or filtered),
an independent typed Tunnel readback, and a Flink SQL aggregate over the result.
Assertions cover exact IDs, duplicate/missing rows, every field value, nulls and counts.
Failed SQL jobs are cancelled before fixture cleanup.

The seven axes are split strategy, compression, batch rows, parallelism, file cache,
query and populated/empty input. The full product has 192 combinations. A deterministic
covering array selects 15 scenarios: every pair is covered; the split/compression/cache
triple is fully covered with populated data; configuration pairs are also covered with
populated data so empty streams cannot satisfy encoding coverage. The actual selected
scenarios are written to `target/sql-parameter-matrix.csv` for review.

Catalog lifecycle templates use primitive/array/struct schema representatives;
projection uses primitive/struct/binary representatives and both split strategies.
Additional journeys cover write modes, partitions, upsert updates/deletes, numeric and
Unicode boundaries, Arrow ownership, public writer APIs and checkpoint recovery.
Scenario counts describe organization, not a production readiness guarantee.

## Concurrent writes and recovery

`EmulatorConcurrentSqlITCase` reuses one Flink SQL `StatementSet` journey with two
output tables. It combines record, file-cached and modern writers, append/PK upsert,
and parallelism 4/8/16. PK cases then update every existing key in a second SQL job
and require the new value for every key. Each output receives 4096 independently verified rows; override
`-Demulator.stress.rows=16384` for a larger run. The emulator defaults to a 30 ms delay on actual data uploads. CI uses
`-Demulator.stress.rows=16384 -Demulator.stress.delay.ms=1000` to populate enough
writer buffers and sustain data-plane overlap on fast runners. Pressure inputs use
preassigned row-offset ranges, so early readers cannot consume the work of readers
that start later. Fault journeys retain dynamic split assignment. The selected
assignment mode and requested delay are retained in each pressure receipt. The assertion requires overlapping **data**
uploads; concurrent metadata or session creation alone does not pass. Evidence records
the measured peak rather than claiming every configured subtask overlaps.
The complete-matrix report additionally requires at least half the configured
parallelism (and at least two) to overlap in actual data uploads for every pressure case.

`EmulatorSqlWriteFaultITCase` injects faults into actual SDK requests for the UUID
output table. Legacy batch paths cover create/write/commit; streaming Tunnel covers
create/write; modern realtime Storage covers create-stream/write.
Modern Storage third-request cases use a 2 KiB writer buffer so multiple uploads occur even when
bounded-source scheduling leaves fewer than three active sink subtasks. The exact
buffer is recorded in the evidence. Legacy Tunnel writers retain their required minimum 1 MiB buffer.
Write faults occur on the first and third
matching upload requests, exercising
recovery while concurrent subtasks may have progressed. Request arrival order is
not treated as proof that the earlier requests have committed. Modern realtime
uses local write-session setup and does not send create-session or batch
close-stream/commit requests: those are not claimed as tested
connector phases. HTTP 503 occurs before mutation; lost ACK disconnects only after
the ordinary service mutation has completed. Retries retain the normal protocol
identity and idempotence rules. Every rule must fire or the test fails.

Jobs have two bounded Flink restarts and a 240-second wait with cancellation on failure.
Pressure uses a 120-second SDK read timeout for the emulator's serialized batch
commits; fault scenarios keep a 3-second timeout to exercise bounded recovery.
Independent typed Tunnel readback verifies every key and value. PK upsert must have
exactly one row per key. Append recovery is **at-least-once**: duplicates may occur
after acknowledged state is replayed or a commit response is lost. The tests verify
complete data and bounded duplicates and record the duplicate count; they do not
assert append exactly-once. Legacy upsert SDK calls have an aggregate deadline of
`read timeout × (max retries + 1)`; expiry interrupts a stuck SDK wait and fails the
operation so Flink can recover. This bounds lost-callback waits in addition to the
SDK network timeouts. Configure this budget for the expected backpressure and batch size. `EmulatorSqlRecoveryITCase` separately injects task failure
after a completed checkpoint and restores real Flink source/operator state.

Resource checks require no active emulator write requests, no remaining connector
writer threads created by the job, and no file-cache artifacts. The fault API is
enabled only on the disposable test service, whose published host port is loopback.

Run the entire acceptance matrix from a clean target:

```sh
python3 tools/run-emulator-tests.py --clean -- -Pcoverage-check
python3 tools/report-readiness.py
```

The report fails if any planned stress/fault case is absent or failed, and writes
`target/readiness/report.md` with parameters, actual concurrency, fault hits, duplicate
counts and duration. Archive this together with the successful build log, JaCoCo XML,
`target/emulator-image.json` and independent emulator conformance results. Local stress
does not establish production throughput or distributed MaxCompute failure behavior;
the real-service suites above remain required for those claims.

The nested-array pipeline uses typed native fixtures followed by Flink SQL
`INSERT SELECT`, independent Tunnel readback and a SQL aggregate. This exercises
arrays of primitive, decimal, Unicode, temporal, binary, map, row and nested-array
values, including null elements and null arrays. In the current Flink version, some
complex `ARRAY` literal expressions fail in planner type conversion or generated
Java compilation before connector execution; they are not claimed as supported
SQL literal construction. The 17-type base journey still uses Flink `INSERT VALUES`.

Pure state tests separately check checkpoint-file replay after transient/exhausted
upload failure, reader closure on each attempt, session publication only on success,
retained state on failure, and source startup/visibility validation. These are
component contracts, not emulator CDC or distributed-service acceptance.

`BucketLateBootstrapContractTest` fixes the checkpoint boundary ordering explicitly:
a session bootstrap arrives after the existing bucket has flushed, while another
bucket is still empty. The existing upload must retain its original session and
commit exactly once; the new empty bucket must still commit. This prevents a late
acknowledgment from causing duplicate file accounting during recovered checkpoints.

## File-cache dynamic partition acceptance

The stress, SDK request fault and checkpoint restore matrices also select
`cached-dynamic` and `grouped-cached-dynamic` (append writes). These select the actual
file-cache sink through Flink SQL; grouped cases additionally enable partition grouping.
Both partition columns (`ds`, `region`) are computed in `INSERT SELECT`. The fixture
produces 21 distinct partition specifications, including null and empty `ds` values
routed to the configured `missing` default and a Unicode/emoji partition. An independent
Java oracle derives the expected partition for every ID and uses typed Tunnel downloads
for every partition. Missing/extra partitions, misplaced IDs, incorrect values and
unbounded duplicates fail the journey; SQL output order is not part of the oracle.

- Pressure: both routing modes at sink parallelism 4/8/16, two output tables with
  4096 rows each and delayed actual upload requests. Reported concurrency must measure
  data uploads, and the complete report requires at least half the configured parallelism.
- Request faults: both routing modes at parallelism 8, 2048 rows, create/write/third
  matching write/commit, each with HTTP 503 before mutation and lost ACK after mutation.
  Additional combinations inject HTTP 429/FlowExceeded at every selected stage,
  and an eight-request 503 burst during writes. The burst is matched across the
  output table, not eight failures in a single SDK session; all eight responses must
  fire and the final data must still converge. Every configured rule must fire. The third request refers to arrival order across the
  target table, not a guarantee that earlier requests committed or the same file was used.
- Checkpoint restore: both routing modes at parallelism 4/8/16, 1024 rows, a completed
  Flink checkpoint followed by a deliberate SQL task failure. The source must restore
  a nonzero checkpoint offset and the restarted sink must preserve every key in its
  expected partition. Delay rules exercise concurrent data uploads during the journey;
  evidence checks completed checkpoint, injected failure, restored state, fault hits,
  per-partition values, and absence of remaining cache files and writer threads.

These add 38 journeys to the existing matrices. `target/readiness/report.md` now checks
all 24 stress, 84 request-fault and 12 checkpoint-restore cases, including the six existing
non-partitioned restore cases. Evidence from filtered probes must not be presented as a
complete matrix run. Append restoration remains at-least-once and permits bounded replay;
pressure without failure requires exactly one row per ID. Dynamic journeys default to one file-cache I/O worker per subtask and two session
commit workers. The published 1.2.0 service has only a 16-request admission budget,
which can throttle two 16-subtask sinks even with one I/O worker each. CI uses
`--build-source --max-concurrent-requests 1024`: the runner checks out the public
repository and immutable commit in `tools/emulator-image.json`, builds its Dockerfile,
and records the actual source commit, image ID and admission budget. It does not
publish the image. The
`emulator.cached.writer.threads` and `emulator.cached.commit.threads` system properties
select other worker budgets; evidence records both. The separate 1.2.1 candidate acceptance
uses the production defaults (16/16) with 1024 admitted requests. It also corrects the
concurrency counter to exclude requests rejected by admission. These tests use local Flink
clusters against a Docker Emulator; separate JM/TM process loss and production savepoint
compatibility remain additional acceptance scopes.
Recovery uses a one-second minimum pause after each completed checkpoint; frequent
barriers must not immediately overlap the closing of the bounded fixture's tasks.

For focused iteration (the full report deliberately rejects this incomplete subset):

```sh
python3 tools/run-emulator-tests.py -- \
  '-Dit.test=EmulatorConcurrentSqlITCase,EmulatorSqlWriteFaultITCase,EmulatorSqlRecoveryITCase' \
  '-Demulator.stress.filter=.*cached-dynamic/.*' \
  '-Demulator.fault.filter=.*cached-dynamic/false/.*' \
  '-Demulator.recovery.filter=.*cached-dynamic/.*'
```

For the admission-capacity candidate (requires the Emulator PR's new CLI flag):

```sh
python3 tools/run-emulator-tests.py --image <candidate-image> --platform linux/arm64 \
  --max-concurrent-requests 1024 --clean -- -Pcoverage-check \
  -Demulator.cached.writer.threads=16 -Demulator.cached.commit.threads=16
python3 tools/report-readiness.py
```

A file-cached shared session spans subtasks. After a writer's real task failure, a
regional retry can leave peers waiting for the failed participant's checkpoint protocol;
the coordinator requests global recovery. Null failure notifications from cancellation
must not request another restart. A coordinator contract separately verifies this and
that checkpoint ACKs skip completed tasks while still reaching pending-end tasks.

## Coordinated Insert mailbox stall

`EmulatorFileCachedMailboxITCase` adds a chained, window-like burst larger than
10,000 rows while session creation is delayed. It covers both initial bootstrap
and the first session rollover after a completed checkpoint, for an unpartitioned
single-subtask sink and three subtasks sharing one existing dynamic partition. The positive
cases select `sink.file-cached.enable=true`; they check every committed key/value,
checkpoint progress, the injected delay, and cleanup of spill files and writer threads.
The dynamic fixture precreates its partition to isolate FileCache's separate
new-partition creation timer, which can already take five seconds. The probe uses
a ten-second checkpoint timeout and a fifteen-second completion deadline. Each subtask
uses one upload worker; the coordinator uses two commit workers to stay within
the pinned Emulator's request-admission budget.

```sh
python3 tools/run-emulator-tests.py --flink-line 1.20 -- \
  -Dit.test=EmulatorFileCachedMailboxITCase
```

The same fixture's opt-in `EmulatorInsertMailboxReproducer` selects the original
coordinated in-memory Insert path. It is a diagnostic reproducer for an unresolved
production-code defect, and is expected to fail on the published baseline. It is
not named `*ITCase` and is not counted as a passing default regression. Run it
explicitly to capture the RUNNING job and its `InsertFunction.processElement`
sleep stack; `target/mailbox-diagnostics/insert-mailbox-*.json` records the observations.

```sh
python3 tools/run-emulator-tests.py --flink-line 1.20 -- \
  -Dit.test=EmulatorInsertMailboxReproducer \
  -Dtest=CachedCoordinatorAckContractTest
# Optional customer-shaped probe: one dynamic partition, three sink subtasks.
python3 tools/run-emulator-tests.py --flink-line 1.20 -- \
  '-Dit.test=EmulatorInsertMailboxReproducer#checkpointRolloverMustProcessNewSessionAckDuringChainedBurst' \
  -Demulator.mailbox.dynamic=true -Demulator.mailbox.parallelism=3 \
  -Dtest=CachedCoordinatorAckContractTest
```

The baseline can starve mailbox processing by sleeping for every row after the
buffer limit. A checkpoint adds a second dependency: Flink can hold the new
session ACK in its coordinator event gateway until the task acknowledges that
checkpoint. Waiting for that ACK inside a chained record callback can prevent
the barrier from reaching the sink, even when the callback yields to its mailbox.
Replacing `sleep` with a blocking ACK wait alone therefore does not resolve the
rollover case. See Flink's [gateway implementation](https://github.com/apache/flink/blob/release-1.20.4/flink-runtime/src/main/java/org/apache/flink/runtime/operators/coordination/SubtaskGatewayImpl.java).

File caching is a verified local workaround for this fixture. It is not a change
to the default writer or a claim about an arbitrary VVR binary/savepoint. A full
in-memory-path repair needs buffering that lets barriers progress, bounded memory
with spill or equivalent flow control, and explicit request/epoch handling for
late ACKs and checkpoint aborts. The original commit/abort protocol must remain
covered; blindly clearing less state or retrying bootstrap changes that protocol.
