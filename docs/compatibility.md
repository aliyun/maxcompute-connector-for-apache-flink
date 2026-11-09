# Compatibility and delivery semantics

## Tested baseline

| Component | Version |
| --- | --- |
| Flink | 1.16.3 / 1.17.2 / 1.20.4 |
| Build/runtime JDK | 17 |
| Maven | 3.9 |
| MaxCompute Java SDK | 0.61.2-public |
| Connector release | 1.0.0 |

Java 8 bytecode is a compiler target, not JDK 8 runtime acceptance. Other Flink
versions and VVR distributions need separate validation. See [multi-version builds](multi-version.md) for the adapter layout and reproducible matrix; recorded historical results below retain their original version scope. Existing deployments must
review serializer, checkpoint and shaded namespace compatibility before upgrading.
The shaded dependency namespace is now `flink.connector.odps.*`; the earlier
`flink-connector-odps.*` prefix generated invalid ServiceLoader provider names.
Connector-facing packages remain `org.apache.flink.odps.*`.

## Write behavior

| Path | Local verification | Recovery semantics and limits |
| --- | --- | --- |
| Legacy coordinated record append/upsert | SQL round trips, concurrent writes, network faults and checkpoints | Append at-least-once; PK tests require complete unique keys and final values |
| Legacy file-cached append/upsert | SQL matrices, spill/replay, reader ownership and cleanup | Append replay may duplicate; local files are not shared durable storage |
| Standalone / Tunnel streaming append | SQL round trips and injected request failures | Independently committed sessions; append at-least-once |
| Modern Storage append/upsert | SQL round trips, concurrent writes and request faults | No recoverable Flink committer state; append at-least-once; PK final-value assertions |

A primary key can make replay idempotent for a tested update sequence; it does not
establish exactly-once execution of arbitrary operations or ordering across independent
jobs. Unsupported modern partial-column updates fail validation. Dynamic partition
null/empty STRING values map to `sink.partition.default-value` before routing.

In the accepted lost-write-ACK run, two modern append cases each returned 2560 rows
for 2048 logical inputs: 512 duplicates and no missing/corrupted values. Consumers
requiring unique events need application-level keys or deduplication.

## Source, Catalog and types

Local suites exercise bounded reads, projection, empty data, partition lifecycle,
Catalog cache invalidation, Arrow/record round trips and checkpoint state contracts.
Lookup joins are unimplemented and rejected. Some complex Flink ARRAY literals fail
in planner/code generation before Connector execution; typed fixtures followed by
SQL `INSERT SELECT` cover their transfer representation separately.

CDC/incremental reads, DLMV, physical bucket/multi-split planning, cloud IAM,
production quota/throughput and distributed service recovery require independent
real-service acceptance. Emulator sessions and staging are process-local; its
single-node transactions do not establish cloud service recovery guarantees.

See [testing](testing.md) for commands and [acceptance results](acceptance.md) for
exact counts, coverage and unresolved gates.


## Choosing the Storage writer

For new ordinary append jobs that accept realtime visibility and at-least-once
replay, we recommend the modern Storage writer. Set the table option
`'sink.coordinator.enable'='false'`. Existing coordinated Insert users should plan
a migration after validating the compatibility points below on their deployment.

`sink.coordinator.enable=false` selects the modern Storage writer. It creates
sessions in each task and avoids the legacy Insert coordinator's session-ID event
handshake. This removes that specific checkpoint-gateway dependency.

The current implementation uses `STREAMING_REALTIME`, Arrow batching and bounded
SDK pending buffers. Flushed data is visible without waiting for the Flink
checkpoint to complete and cannot be rolled back as a batch. Legacy Tunnel Upload
Insert instead commits its upload session during checkpoint or bounded completion.
Neither path provides general append exactly-once delivery. See the SDK's
[write-mode contract](https://aliyun.github.io/aliyun-odps-java-sdk/reference/TableWriteSession/).

Modern Storage supports ordinary append and full-row primary-key upsert, but
partial-column upsert is explicitly rejected. The latter is a reason to retain
legacy upsert support; it does not by itself prevent migrating ordinary Insert
jobs. Checkpoint-aligned batch visibility is a separate compatibility decision
for Insert consumers.

Changing the option also changes the operator and writer state layout. Do not
assume an existing legacy savepoint is directly compatible. Validate migration,
source replay and append deduplication on the target runtime. Validate Storage
availability, permissions, quotas and performance with the actual service before
changing a production default. Local Emulator throughput cannot establish a
cloud performance advantage.
