# Configuration

The SQL factory identifier is `maxcompute`. The complete option definitions are in
[OdpsOptions](../src/main/java/org/apache/flink/odps/table/OdpsOptions.java); the factory's
required and optional sets define the accepted SQL keys. Keys are case-sensitive.
This guide covers the common configuration surface.

## Connection and credentials

| Key | Default | Meaning |
| --- | --- | --- |
| `table.name` | Required | `project.table` or schema-qualified table identifier |
| `odps.project.name` | None | Default project |
| `odps.end.point` | None | ODPS service endpoint |
| `odps.tunnel.end.point` | Empty | Explicit Tunnel endpoint, when required |
| `odps.tunnel.quota.name` | None | Named Tunnel quota |
| `odps.namespace.schema` | `false` | Enable schema namespace |
| `odps.credentials.provider` | None | Class implementing `com.aliyun.credentials.api.ICredentialsProvider` |
| `odps.credentials.provider.params` | None | Provider parameters |
| `odps.access.id`, `odps.access.key` | None | Explicit credentials; dummy values in local tests only |

The provider must be present on every worker. It is instantiated with a no-argument
constructor when parameters are absent, or a `Map<String, String>` constructor when
provided. Credential creation failures fail the job. Configure credentials through
the deployment's secret mechanism and keep them out of committed SQL and logs.

## Source

| Key | Default | Meaning |
| --- | --- | --- |
| `splitstrategy` | `ROW_OFFSET` | `ROW_OFFSET` or `BYTE_SIZE` planning |
| `splitbytesize` | 268435456 | Requested byte-size split target |
| `splitbatchreadsize` | 4096 | Rows per read batch |
| `compressCodec` | `NO_COMPRESSION` | SDK Arrow compression enum; use the exact key spelling |
| `dynamicloadbalancing` | `false` | SDK read-session dynamic load balancing |
| `scan.partition` | None | Explicit partition selection |
| `scan.mode` | `LATEST` | Startup mode; bounded jobs and incremental jobs have different contracts |
| `scan.incremental.mode` | `APPEND` | Incremental read mode |
| `scan.partition.latest` | `false` | Opt-in latest-partition discovery for supported PK CDC tables |
| `scan.partition.ready-mode` | None | Latest discovery requires `partition-visible` |
| `scan.disk-spill.enabled` | `false` | Optional disk-spill path; requires compatible runtime capabilities |

Incremental windows and latest-partition discovery require service CDC support.
Latest-partition publication must be ordered and data/CDC ready when visible;
switching partitions stops reading the old partition. See the implementation and
real-service suites before using this mode. Emulator batch reads do not prove CDC.

## Sink

| Key | Default | Meaning |
| --- | --- | --- |
| `sink.operation` | `insert` | Legacy `insert` or `upsert` operation |
| `sink.parallelism` | None | Explicit sink parallelism |
| `sink.coordinator.enable` | `true` | Legacy coordinated writer; `false` selects the modern Storage writer |
| `sink.file-cached.enable` | `false` | Legacy file-cached writer |
| `sink.standalone.enable` | `false` | Legacy independent subtask sessions |
| `insert.streaming-tunnel.enable` | `false` | Tunnel streaming append; auto-enables standalone mode |
| `sink.partition` | None | Static partition specification |
| `sink.partition.default-value` | `__DEFAULT_PARTITION__` | Dynamic STRING null/empty partition replacement |
| `insert.writer.buffer-size` | `64mb` | Writer buffering; legacy Tunnel buffered writers require at least 1 MiB |
| `sink.read.timeout` | `120 s` | SDK network read timeout |
| `sink.flush.timeout` | `30 s` | Flush timeout |
| `sink.dynamic-shuffle.enable` | `false` | Opt-in partition/bucket assignment balancing |
| `upsert.partial-column.enable` | `false` | Legacy partial-column updates; modern path rejects this mode |

Writer selection and option applicability vary by path. Modern Storage detects the
backing primary key; it does not use the legacy partial-column path. For production
partitioned PK correctness, leave `sink.shuffle.bucket-only.enable=false`; its
experimental bucket-only routing does not preserve partition/bucket affinity.

Size timeouts for actual backpressure and batch sizes. Legacy upsert SDK waits are
bounded by `read timeout × (max retries + 1)`; timeout failure allows Flink to recover.
A retry can duplicate append records. See [compatibility](compatibility.md) and
[testing](testing.md) for the tested combinations and recovery guarantees.
