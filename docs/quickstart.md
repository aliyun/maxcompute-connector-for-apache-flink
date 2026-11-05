# Flink SQL quickstart

Use the [tested runtime baseline](compatibility.md) and the shaded jar built in
[README](../README.md). Start with a disposable local service before using cloud tables.

## Local table

Use [MaxCompute Emulator](https://github.com/dingxin-tech/maxcompute-emulator) 1.2.0,
which includes Storage v1 and write-fault support. The test runner pins its exact
image digest in [tools/emulator-image.json](../tools/emulator-image.json). This manual
example uses the version tag; automated tests use the digest.

```sh
cat > sample.sql <<'SQL'
CREATE TABLE sample (id BIGINT, v STRING);
SQL
docker run --rm --platform linux/amd64 -p 127.0.0.1:8080:8080 \
  -v "$PWD/sample.sql:/fixtures.sql:ro" maxcompute/maxcompute-emulator:1.2.0 \
  --listen 0.0.0.0:8080 --project test_project --seed /fixtures.sql
```

In Flink SQL, using the same schema:

```sql
SET 'execution.runtime-mode' = 'batch';

CREATE TABLE mc_sample (id BIGINT, v STRING) WITH (
  'connector' = 'maxcompute',
  'table.name' = 'test_project.sample',
  'odps.project.name' = 'test_project',
  'odps.end.point' = 'http://127.0.0.1:8080',
  'odps.tunnel.end.point' = 'http://127.0.0.1:8080',
  'odps.access.id' = 'test-ak',
  'odps.access.key' = 'test-sk'
);

INSERT INTO mc_sample VALUES (1, 'hello'), (2, 'MaxCompute');
SELECT id, v FROM mc_sample;
```

Wait for the insert job to finish before starting the read job. The expected rows
are `(1, 'hello')` and `(2, 'MaxCompute')`; row order is unspecified. Dummy credentials
are only for the permissive local Emulator. The endpoint must be reachable from
both JobManager and TaskManagers; use a container-network hostname when appropriate.

## Real MaxCompute

Create a disposable table in an approved project with the same schema. Replace the
project/table and endpoints above with the actual service values, and use a
credentials provider available on every Flink worker. See [configuration](configuration.md).
Inline cloud credentials can enter SQL logs, execution plans and checkpoints; use
the deployment's secret/provider mechanism. Environment variables in a SQL string
are not automatically expanded by this Connector.

For primary-key tables, declare the matching Flink columns `NOT NULL` and
`PRIMARY KEY (...) NOT ENFORCED`. Enable `sink.operation=upsert` for the legacy
coordinator path. Keep the backing primary key, schema and partition specification
consistent with Flink. Review [delivery semantics](compatibility.md) before enabling
restarts or using a different writer path.

Run the [real-service acceptance suites](testing.md) for the intended deployment.
The local example validates an append/read journey, not cloud IAM, quota, CDC or
production throughput.
