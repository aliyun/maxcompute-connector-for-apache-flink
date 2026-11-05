# Shared implementation and Flink runtime lines

One source branch builds three distinct distributions. The Connector feature version is independent of the Flink runtime line; never overwrite an existing release to add another implementation.

| Module / artifactId suffix | Pinned Flink dependency | Verification JDK |
| --- | --- | --- |
| `flink/1.16` / `flink-connector-maxcompute-1.16` | 1.16.3 | 17 |
| `flink/1.17` / `flink-connector-maxcompute-1.17` | 1.17.2 | 17 |
| `flink/1.20` / `flink-connector-maxcompute-1.20` | 1.20.4 | 17 |

## Source layout

```text
src/
  main/
    java/                         # Shared Storage, shuffle, Source/Sink/Catalog
    resources/                    # SPI and connector resources
    shims/
      flink-1.16-1.17/java/        # Adapter used by both runtime lines
      flink-1.20/java/             # WriterInitContext and cache-view ABI adapter
  test/                           # Shared regressions and fixtures
flink/
  1.16/pom.xml
  1.17/pom.xml
  1.20/pom.xml
```

The three directories under `flink/` are Maven distribution modules. Each compiles the same `src/main/java` and `src/test/java` against its own Flink dependencies. The parent declares the shared source and resource directories; build-helper adds exactly one shim source set per module. There is no separately distributed common binary, and no business code in the version modules.

Storage API v2 session ownership, Arrow batching, asynchronous flush/backpressure and cleanup live in the `org.apache.flink.odps.storage` package. It imports no Flink APIs; record conversion is injected through an encoder. The shared assignment algorithm, partition/bucket keys, statistics, assignment model and runtime protocol live in `org.apache.flink.odps.sink.shuffle`. Existing package names and serialVersionUID values are retained.

The file-cache seam adapts credit-count versus boolean availability and the listener callback signature. No write, shuffle or recovery algorithm is copied into version directories. No runtime reflection selects an adapter. Flink dependencies remain provided; the public profile packages and relocates the SDK and retains SPI/legal resources. SDK-side commons-compress, commons-lang3 and slf4j-api resolution is pinned to the existing reviewed baseline across all three runtimes.

## Build and verify

```sh
# All three lines: clean compile, offline/MiniCluster recovery, sources/javadoc,
# independent shaded-jar consumer, dependency/legal closure and result JSON.
python3 tools/verify-matrix.py

# One line, suitable for a CI matrix cell.
python3 tools/verify-matrix.py --flink-line 1.16

# Faster development loop for one target.
mvn -s tools/maven-central-settings.xml -gs tools/maven-central-settings.xml \
  -pl flink/1.17 -am clean verify

# Loopback Emulator: same selected runtime and existing SQL/fault suites.
python3 tools/run-emulator-tests.py --flink-line 1.20 --clean -- -Pcoverage-check
python3 tools/report-readiness.py --flink-line 1.20
```

Each child writes to its own `flink/<line>/target/`. For example, the 1.16 shaded candidate is `flink/1.16/target/flink-connector-maxcompute-1.16-1.20.0-shaded.jar`. Install only the matching runtime's jar in a cluster.

The matrix runner retains logs and a JSON receipt under `.verification/matrix/`, outside Maven clean directories. It rejects missing/skipped/failed regressions, absent MiniCluster recovery, absent Storage lifecycle tests, wrong runtime/artifact identity and divergent shared class hashes. The distributed consumer loads the actual jar with provided Flink dependencies, not target/classes or the unshaded SDK.

JaCoCo instruments all Connector-owned `org.apache.flink.odps.*` classes. It does not instrument third-party planner internals: Flink 1.17 Calcite reflects over declared metadata-handler methods and rejects JaCoCo's extra synthetic method. The production coverage report and 80% gate retain all Connector packages. The offline planner-bootstrap regression exercises this under every runtime.

## Compatibility limits

Version adapters preserve the existing append at-least-once and primary-key final-value semantics. Extracting the common writer adds no recoverable Flink committer and establishes no new exactly-once guarantee. Existing partial-column validation remains in the shared factory.

JDK 17 is the tested build/runtime baseline for this change. Java 8 bytecode does not prove Java 8/11 runtime compatibility. Matching serializer class names does not prove cross-Flink savepoint upgrades; topology UIDs, state restoration and upgrade paths require separate acceptance. Flink 1.14 and 2.0 are outside this implementation.

Local compilation, offline/MiniCluster recovery, Emulator contracts, online CI, real cloud service behavior and artifact publication are separate results. The existing bundled-binary redistribution and real-service release gates still apply.
