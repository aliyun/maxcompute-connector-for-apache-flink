# MaxCompute Connector for Apache Flink®

Read and write Alibaba Cloud MaxCompute tables with Apache Flink SQL, Table API,
DataStream API and Catalog integration.

[![Connector verification](https://github.com/aliyun/maxcompute-connector-for-apache-flink/actions/workflows/ci.yml/badge.svg)](https://github.com/aliyun/maxcompute-connector-for-apache-flink/actions/workflows/ci.yml)

The supported build baselines are **Flink 1.16.3 / 1.17.2 / 1.20.4,
JDK 17, Maven 3.9 and MaxCompute Java SDK 0.61.2-public**. Java 8 bytecode is a compiler
target; other Flink/JDK combinations require separate acceptance.

## Get started

Build the connector with JDK 17:

```sh
git clone https://github.com/aliyun/maxcompute-connector-for-apache-flink.git
cd maxcompute-connector-for-apache-flink
python3 tools/verify-matrix.py
```

The `public` profile builds `flink/<line>/target/<artifactId>-<version>-shaded.jar`
with its SDK dependencies and verifies it in an isolated consumer. Install that jar
in the Flink cluster's `lib/` directory and restart the cluster. Flink dependencies
are provided by the runtime. The version is the current source version; this guide
does not imply that it has been published to Maven Central.

See [multi-version builds](docs/multi-version.md) for source sharing, version selection and CI.

Follow the [SQL quickstart](docs/quickstart.md) for a local Emulator example and
real-service setup. For credentials, partitions, source planning and sink selection,
see [configuration](docs/configuration.md).

## Capabilities and guarantees

- Bounded table reads, column projection and partition selection.
- Append writes and primary-key upsert/delete, with record, file-cached and Storage writers.
- Catalog metadata and table/partition lifecycle operations.
- Opt-in continuous primary-key reads and latest-partition discovery, requiring
  service CDC capabilities and separate real-service acceptance.

Append recovery is **at-least-once** and may produce duplicates after a lost
acknowledgment. Primary-key tests verify final values and unique keys; this is not
an end-to-end exactly-once claim for every writer and source combination.
Lookup joins are currently unimplemented. See [compatibility and delivery semantics](docs/compatibility.md).

## Documentation

| Guide | Purpose |
| --- | --- |
| [SQL quickstart](docs/quickstart.md) | Build, install, append and read a table |
| [Configuration](docs/configuration.md) | Credentials, source and sink options |
| [Compatibility](docs/compatibility.md) | Runtime baseline, supported paths and delivery semantics |
| [Testing](docs/testing.md) | Offline, Emulator, stress, fault recovery and real-service suites |
| [Acceptance results](docs/acceptance.md) | Reproducible local results and remaining acceptance gates |
| [Contributing](CONTRIBUTING.md) | Development and review expectations |
| [Release procedure](docs/releasing.md) | Source export, approval, signing and artifact verification |
| [Third-party material](licenses/README.md) | Exact dependency closure and redistribution review |

## Development

```sh
# Offline contracts; no service credentials
mvn -s tools/maven-central-settings.xml -gs tools/maven-central-settings.xml clean verify

# Complete Docker-backed SQL matrix and 80% production line coverage gate
python3 tools/run-emulator-tests.py --build-source --max-concurrent-requests 1024 --clean -- \
  -Pcoverage-check -Demulator.stress.rows=16384 -Demulator.stress.delay.ms=1000
python3 tools/report-readiness.py
```

The default Emulator is the released 1.2.0 image, pinned by digest in
[tools/emulator-image.json](tools/emulator-image.json), with Storage v1 and write-fault
support. CI uses `--build-source` to build the locked public admission-capacity fix,
with enough admitted requests for both sinks; the image is never published. Local
development can select the same build or use `--image`. See [testing](docs/testing.md) for commands,
independent protocol conformance and real-service requirements.

## License and release status

Connector code is licensed under [Apache-2.0](LICENSE); upstream attribution is
retained in source and [NOTICE](NOTICE). Bundled libraries retain their own licenses,
listed in the [dependency catalog](licenses/third-party.json).

The connector source is available in this repository. Maven Central publication and
distribution of bundled binary artifacts require signing, namespace authorization,
real-service acceptance and third-party redistribution review. CI retains test reports.
See the [release procedure](docs/releasing.md).

## Trademarks

Apache®, [Apache Flink®](https://flink.apache.org/), and Flink® are registered
trademarks of [The Apache Software Foundation](https://www.apache.org/).

This connector is maintained by the Alibaba Cloud MaxCompute team. It has no
affiliation with or endorsement from the Apache Software Foundation or the
Apache Flink project.
