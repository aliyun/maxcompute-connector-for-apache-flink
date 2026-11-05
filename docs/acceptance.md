# Verification scope

The [Connector verification workflow](https://github.com/aliyun/maxcompute-connector-for-apache-flink/actions/workflows/ci.yml)
runs on pushes to `main`, pull requests and manual dispatch. Each run identifies the
exact public commit, runtime line and reports; consult that run for its actual result.

| Gate | Scope |
| --- | --- |
| Source checks | Gitleaks and checks for private endpoints, local paths and sensitive files; no cloud identity |
| Offline and distributed jar | Flink 1.16.3, 1.17.2 and 1.20.4 on JDK 17; state/contracts, MiniCluster recovery, Storage lifecycle and an isolated shaded-jar consumer |
| Emulator | The same three runtimes; Docker SQL reads/writes, dynamic partitions, concurrent writes, request faults and checkpoint recovery |
| Coverage | Clean Emulator runs enforce 80% production line coverage across all Connector packages |
| Dependency closure | The public dependency graph must match the retained third-party catalog and notices |

Run these checks locally with the commands in [testing](testing.md) and
[multi-version builds](multi-version.md). Archive Surefire/Failsafe XML, JaCoCo,
SQL parameter cases, readiness reports and image identity before another build
overwrites the selected module's `target/` directory. CI artifacts retain these
reports for 14 days; bundled jars are not published by the verification workflow.

CI pressure cases use 16384 rows per output and a 1000 ms delay on actual data
uploads. Pressure scans preassign row ranges across all readers to avoid starvation
from startup timing. The measured overlap must still reach at least half the configured
parallelism; record/partition correctness and resource cleanup remain required.

The pinned Emulator reference and platform are in
[tools/emulator-image.json](../tools/emulator-image.json). Its runner starts an isolated
loopback service with dummy credentials and removes the container in `finally`.
CI builds the locked public source commit with its admission-capacity fix and admits
1024 requests, so a 16-request service limit cannot turn the no-failure pressure test
into a throttling test. The image stays local to the runner. Request-fault journeys
still inject and verify explicit throttling and failures. Recovery checkpoints retain
a one-second minimum pause after completion before the next barrier.

Append recovery is at-least-once and can produce bounded duplicates after a lost
acknowledgment. Primary-key tests check final values and key uniqueness. These
checks do not establish end-to-end exactly-once delivery for every configuration.

Cloud CDC/DLMV, physical split planning, IAM/quota, distributed service recovery and
cross-version savepoint upgrades need separate real-service acceptance. JavaCsv 2.0
redistribution provenance, signing and Maven Central publication remain binary
release gates described in [releasing](releasing.md).
