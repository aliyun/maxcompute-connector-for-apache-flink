# MaxCompute Connector for Apache Flink®

Read and write Alibaba Cloud MaxCompute tables with Apache Flink SQL, Table API,
DataStream API and Catalog integration.

[![Connector verification](https://github.com/aliyun/maxcompute-connector-for-apache-flink/actions/workflows/ci.yml/badge.svg)](https://github.com/aliyun/maxcompute-connector-for-apache-flink/actions/workflows/ci.yml)

The supported build baselines are **Flink 1.16.3 / 1.17.2 / 1.20.4,
JDK 17, Maven 3.9 and MaxCompute Java SDK 0.61.2-public**. Java 8 bytecode is a compiler
target; other Flink/JDK combinations require separate acceptance.

## Get started

Follow [Build from source](#build-from-source) to create a shaded JAR for your
Flink version, then [install it in your cluster](#install-in-your-flink-cluster).
The [SQL quickstart](docs/quickstart.md) covers a local Emulator example and
real-service setup. For credentials, partitions, source planning and sink selection,
see [configuration](docs/configuration.md).

## Build from source

Install **JDK 17, Maven 3.9 and Git**. Set `JAVA_HOME` to your JDK 17 installation.
Clone the repository and check that `mvn -version` reports Java 17:

```sh
git clone https://github.com/aliyun/maxcompute-connector-for-apache-flink.git
cd maxcompute-connector-for-apache-flink
mvn -version
```

From the repository root, choose the command matching your cluster's Flink runtime
line:

```sh
# Flink 1.16 (tested with 1.16.3)
mvn -s tools/maven-central-settings.xml -gs tools/maven-central-settings.xml \
  -Ppublic -pl flink/1.16 -am clean verify

# Flink 1.17 (tested with 1.17.2)
mvn -s tools/maven-central-settings.xml -gs tools/maven-central-settings.xml \
  -Ppublic -pl flink/1.17 -am clean verify

# Flink 1.20 (tested with 1.20.4)
mvn -s tools/maven-central-settings.xml -gs tools/maven-central-settings.xml \
  -Ppublic -pl flink/1.20 -am clean verify
```

`-Ppublic` enables shading and bundles the MaxCompute SDK and connector dependencies;
Flink dependencies remain provided by the runtime. `-pl` selects the version module
and `-am` includes its parent in the build. The supplied Maven settings resolve
dependencies from Maven Central. These commands run offline tests and verify the
packaged JAR in an isolated consumer; they require no MaxCompute credentials,
Docker or signing keys.

For Connector release `1.0.1`, the resulting deployment artifacts are:

| Flink runtime line | Shaded JAR |
| --- | --- |
| 1.16 | `flink/1.16/target/flink-connector-maxcompute-1.0.1-1.16-shaded.jar` |
| 1.17 | `flink/1.17/target/flink-connector-maxcompute-1.0.1-1.17-shaded.jar` |
| 1.20 | `flink/1.20/target/flink-connector-maxcompute-1.0.1-1.20-shaded.jar` |

Use the file ending in `-shaded.jar` for cluster installation. The JAR without that
suffix does not bundle the SDK dependencies. The Connector release (`1.0.1`) and
Flink runtime line (`1.16`, `1.17` or `1.20`) are separate parts of the artifact
version. See [multi-version builds](docs/multi-version.md) for source sharing and
the complete verification matrix.

## Install in your Flink cluster

Set `FLINK_HOME` to your Flink installation directory and copy the matching shaded
JAR into its `lib/` directory. For example, for Flink 1.20:

```sh
cp flink/1.20/target/flink-connector-maxcompute-1.0.1-1.20-shaded.jar "$FLINK_HOME/lib/"
```

Install the same JAR on the JobManager, every TaskManager and any separate SQL
Client or SQL Gateway installation. Keep only one version of this connector on
each classpath, replacing any older connector JAR. Restart the affected Flink
processes using your deployment's normal procedure so they load the new library.

With the JAR installed, start the SQL Client with `"$FLINK_HOME/bin/sql-client.sh"`
and follow the [SQL quickstart](docs/quickstart.md). For other dependency-loading
options, see the [Flink SQL Client documentation](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sqlclient/#dependencies).

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
| [Build from source](#build-from-source) | Build a shaded JAR for Flink 1.16, 1.17 or 1.20 and install it in a cluster |
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
Emulator acceptance on all three runtimes and third-party redistribution review.
CI retains test reports.
See the [release procedure](docs/releasing.md).

## Trademarks

Apache®, [Apache Flink®](https://flink.apache.org/), and Flink® are registered
trademarks of [The Apache Software Foundation](https://www.apache.org/).

This connector is maintained by the Alibaba Cloud MaxCompute team. It has no
affiliation with or endorsement from the Apache Software Foundation or the
Apache Flink project.
