/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.odps.test.emulator;

import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;
import com.google.gson.*;

import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.table.api.*;
import org.junit.Assert;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Reusable SQL stress/fault journey with independent readback and persistent evidence. */
public abstract class EmulatorWriteHarness extends EmulatorFixture {
    protected EmulatorWriteHarness() {
        super("writevalidation", "BIGINT", "BIGINT", "42", "42");
    }

    protected void writeScenario(
            String label,
            String writer,
            boolean upsert,
            int parallelism,
            int records,
            String stage,
            String effect,
            int outputs)
            throws Exception {
        boolean dynamic = writer.contains("dynamic");
        boolean grouped = writer.startsWith("grouped-");
        writer = writer.replace("grouped-", "").replace("-dynamic", "");
        Path temporary = Files.createTempDirectory("flink-odps-stress-");
        List<String> tables = new ArrayList<>();
        List<String> rules = new ArrayList<>();
        Map<String, Object> evidence = new LinkedHashMap<>();
        Set<Long> originalThreads = writerThreads().keySet();
        Throwable primaryFailure = null;
        long started = System.nanoTime();
        evidence.put("scenario", label);
        evidence.put("writer", writer);
        evidence.put("upsert", upsert);
        evidence.put("dynamic_partitions", dynamic);
        evidence.put("group_partitions", grouped);
        if (dynamic) {
            evidence.put(
                    "cached_writer_threads",
                    Integer.getInteger("emulator.cached.writer.threads", 1));
            evidence.put(
                    "cached_commit_threads",
                    Integer.getInteger("emulator.cached.commit.threads", 2));
        }
        evidence.put("parallelism", parallelism);
        evidence.put("rows_per_output", records);
        evidence.put("outputs", outputs);
        evidence.put("stage", stage);
        evidence.put("effect", effect);
        // A shared split queue can be drained by early readers. Pressure needs work on every
        // reader; fault journeys keep dynamic assignment as a separate recovery dimension.
        boolean dynamicSourceAssignment = !effect.equals("delay");
        evidence.put("source_dynamic_load_balancing", dynamicSourceAssignment);
        int uploadDelayMillis = Integer.getInteger("emulator.stress.delay.ms", 30);
        if (effect.equals("delay")) evidence.put("data_upload_delay_ms", uploadDelayMillis);
        // Small Storage buffers force multiple actual uploads even if bounded-source
        // scheduling leaves fewer than three sink subtasks with data. Tunnel
        // buffered writers require at least 1 MiB and retain their normal setting.
        String writerBufferSize =
                writer.equals("modern") && stage.equals("write_late") ? "2 kb" : "1 mb";
        evidence.put("writer_buffer_size", writerBufferSize);
        try {
            StringBuilder values = new StringBuilder();
            for (int i = 0; i < records; i++) {
                if (i > 0) values.append(',');
                values.append('(').append(i).append(',').append(i * 7L + 3).append(')');
            }
            sql("INSERT INTO " + table + " VALUES " + values);
            control("POST", "/__test/write-metrics", "{}");
            TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inBatchMode());
            env.getConfig().getConfiguration().setInteger("parallelism.default", parallelism);
            env.getConfig()
                    .getConfiguration()
                    .set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
            env.getConfig()
                    .getConfiguration()
                    .setInteger("restart-strategy.fixed-delay.attempts", 2);
            env.getConfig()
                    .getConfiguration()
                    .setString("restart-strategy.fixed-delay.delay", "100 ms");
            createSqlTable(
                    env,
                    "stress_input",
                    table,
                    false,
                    false,
                    writer,
                    parallelism,
                    temporary,
                    effect.equals("delay") ? 120 : 3,
                    writerBufferSize,
                    false,
                    false,
                    dynamicSourceAssignment);
            StatementSet statement = env.createStatementSet();
            for (int output = 0; output < outputs; output++) {
                String destination = table + "_out" + output;
                tables.add(destination);
                sql(
                        "CREATE TABLE "
                                + destination
                                + "(id BIGINT,v BIGINT"
                                + (upsert
                                        ? ",PRIMARY KEY(id))"
                                              + " TBLPROPERTIES('transactional'='true','write.bucket.num'='16')"
                                        : ")")
                                + (dynamic ? " PARTITIONED BY(ds STRING,region STRING)" : ""));
                createSqlTable(
                        env,
                        "stress_output" + output,
                        destination,
                        true,
                        upsert,
                        writer,
                        parallelism,
                        temporary,
                        effect.equals("delay") ? 120 : 3,
                        writerBufferSize,
                        dynamic,
                        grouped,
                        dynamicSourceAssignment);
                if (!stage.equals("none")) {
                    String rule = destination + "_fault";
                    rules.add(rule);
                    Map<String, Object> match = new LinkedHashMap<>();
                    match.put("plane", writer.equals("modern") ? "storage" : "tunnel");
                    match.put(
                            "object",
                            writer.equals("modern")
                                    ? "write"
                                    : upsert
                                            ? "upsert"
                                            : writer.equals("stream-record") ? "stream" : "upload");
                    match.put("action", stage.equals("write_late") ? "write" : stage);
                    if (stage.equals("write_late")) match.put("attempt", 3);
                    match.put("project", "test_project");
                    match.put("table", destination);
                    Map<String, Object> failure = new LinkedHashMap<>();
                    failure.put(
                            "type",
                            effect.equals("http_error_exhausted") || effect.equals("http_throttle")
                                    ? "http_error"
                                    : effect);
                    if (effect.startsWith("http_error") || effect.equals("http_throttle")) {
                        failure.put("status", effect.equals("http_throttle") ? 429 : 503);
                        failure.put(
                                "code",
                                effect.equals("http_throttle")
                                        ? "FlowExceeded"
                                        : "ServiceUnavailable");
                    }
                    if (effect.equals("delay")) failure.put("delay_ms", uploadDelayMillis);
                    Map<String, Object> config = new LinkedHashMap<>();
                    config.put("match", match);
                    config.put("effect", failure);
                    config.put(
                            "times",
                            effect.equals("delay")
                                    ? 10000
                                    : effect.equals("http_error_exhausted") ? 8 : 1);
                    config.put("ttl_seconds", 600);
                    control("PUT", "/__test/faults/" + rule, new Gson().toJson(config));
                }
                statement.addInsertSql(
                        "INSERT INTO stress_output"
                                + output
                                + " SELECT id,v"
                                + (dynamic ? "," + CachedDynamicPartitions.sqlColumns() : "")
                                + " FROM stress_input");
            }
            awaitWrite(statement.execute());
            boolean rewrite = upsert && effect.equals("delay");
            if (rewrite) {
                StatementSet updates = env.createStatementSet();
                for (int output = 0; output < outputs; output++) {
                    updates.addInsertSql(
                            "INSERT INTO stress_output"
                                    + output
                                    + " SELECT id,v+1"
                                    + (dynamic ? "," + CachedDynamicPartitions.sqlColumns() : "")
                                    + " FROM stress_input");
                }
                awaitWrite(updates.execute());
            }
            evidence.put("write_rounds", rewrite ? 2 : 1);
            List<Long> counts = new ArrayList<>();
            List<Long> duplicates = new ArrayList<>();
            for (String destination : tables) {
                TableTunnel tunnel = new TableTunnel(odps);
                tunnel.setEndpoint(endpoint);
                long count;
                if (dynamic) {
                    count =
                            CachedDynamicPartitions.verify(
                                    odps,
                                    tunnel,
                                    destination,
                                    records,
                                    rewrite,
                                    upsert || effect.equals("none") || effect.equals("delay"));
                } else {
                    TableTunnel.DownloadSession session =
                            tunnel.createDownloadSession("test_project", destination);
                    count = session.getRecordCount();
                    Set<Long> ids = new HashSet<>();
                    try (TunnelRecordReader reader = session.openRecordReader(0, count)) {
                        com.aliyun.odps.data.Record row;
                        while ((row = reader.read()) != null) {
                            long id = row.getBigint(0);
                            Assert.assertTrue("unexpected id " + id, id >= 0 && id < records);
                            Assert.assertEquals(
                                    id * 7 + (rewrite ? 4 : 3), row.getBigint(1).longValue());
                            ids.add(id);
                        }
                    }
                    Assert.assertEquals("missing records", records, ids.size());
                    if (upsert || effect.equals("none") || effect.equals("delay"))
                        Assert.assertEquals("unexpected duplicate", records, count);
                    else
                        Assert.assertTrue(
                                "unbounded retries", count >= records && count <= records * 4L);
                }
                counts.add(count);
                duplicates.add(count - records);
            }
            if (dynamic)
                evidence.put(
                        "verified_partitions_per_output",
                        CachedDynamicPartitions.partitionCount(records));
            evidence.put("record_counts", counts);
            evidence.put("duplicate_counts", duplicates);
            List<Integer> hits = new ArrayList<>();
            for (String rule : rules) {
                int hit = control("GET", "/__test/faults/" + rule, null).get("hits").getAsInt();
                Assert.assertTrue("fault did not fire: " + label, hit > 0);
                if (effect.equals("http_error_exhausted"))
                    Assert.assertEquals("burst not fully exercised", 8, hit);
                hits.add(hit);
            }
            evidence.put("fault_hits", hits);
            JsonObject metrics = control("GET", "/__test/write-metrics", null);
            Assert.assertEquals("write requests still active", 0, metrics.get("active").getAsInt());
            evidence.put("peak_write_requests", metrics.get("peak").getAsInt());
            evidence.put("peak_data_writes", metrics.get("peak_data").getAsInt());
            if (effect.equals("delay"))
                Assert.assertTrue(
                        "no actual concurrent data uploads",
                        metrics.get("peak_data").getAsInt() >= 2);
            if (writer.equals("cached")) {
                try (java.util.stream.Stream<Path> files = Files.walk(temporary)) {
                    Assert.assertEquals(
                            "cache files leaked", 0L, files.filter(Files::isRegularFile).count());
                }
            }
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            Map<Long, String> remaining;
            do {
                remaining = writerThreads();
                remaining.keySet().removeAll(originalThreads);
                if (remaining.isEmpty()) break;
                Thread.sleep(25);
            } while (System.nanoTime() < deadline);
            Assert.assertTrue("writer threads leaked: " + remaining, remaining.isEmpty());
            evidence.put("remaining_writer_threads", remaining.size());
            evidence.put("status", "PASS");
        } catch (Throwable failure) {
            primaryFailure = failure;
            evidence.put("status", "FAIL");
            evidence.put("error", failure.toString());
            throw failure;
        } finally {
            List<Throwable> cleanupFailures = new ArrayList<>();
            for (String rule : rules)
                cleanup(cleanupFailures, () -> control("DELETE", "/__test/faults/" + rule, null));
            for (String destination : tables)
                cleanup(
                        cleanupFailures,
                        () -> {
                            sql("DROP TABLE IF EXISTS " + destination);
                            Assert.assertFalse(
                                    "output table leaked",
                                    odps.tables().exists("test_project", destination));
                        });
            cleanup(
                    cleanupFailures,
                    () -> {
                        try (java.util.stream.Stream<Path> files = Files.walk(temporary)) {
                            for (Path path :
                                    files.sorted(Comparator.reverseOrder())
                                            .collect(java.util.stream.Collectors.toList()))
                                cleanup(cleanupFailures, () -> Files.deleteIfExists(path));
                        }
                    });
            evidence.put(
                    "cleanup_failures",
                    cleanupFailures.stream()
                            .map(Throwable::toString)
                            .collect(java.util.stream.Collectors.toList()));
            if (!cleanupFailures.isEmpty()) {
                evidence.put("status", "FAIL");
                evidence.put("cleanup_error", cleanupFailures.get(0).toString());
            }
            evidence.put("duration_ms", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
            Path report =
                    Paths.get(
                            "target/readiness", label.replaceAll("[^A-Za-z0-9_-]", "_") + ".json");
            cleanup(
                    cleanupFailures,
                    () -> {
                        Files.createDirectories(report.getParent());
                        Files.write(
                                report,
                                new GsonBuilder()
                                        .setPrettyPrinting()
                                        .create()
                                        .toJson(evidence)
                                        .getBytes(StandardCharsets.UTF_8));
                    });
            if (!cleanupFailures.isEmpty()) {
                if (primaryFailure != null) {
                    for (Throwable cleanupFailure : cleanupFailures)
                        primaryFailure.addSuppressed(cleanupFailure);
                } else {
                    AssertionError failure =
                            new AssertionError("Scenario cleanup failed: " + label);
                    for (Throwable cleanupFailure : cleanupFailures)
                        failure.addSuppressed(cleanupFailure);
                    throw failure;
                }
            }
        }
    }

    private interface Cleanup {
        void run() throws Exception;
    }

    private static void cleanup(List<Throwable> failures, Cleanup action) {
        try {
            action.run();
        } catch (Throwable failure) {
            failures.add(failure);
        }
    }

    private static void awaitWrite(TableResult job) throws Exception {
        try {
            job.await(240, TimeUnit.SECONDS);
        } catch (Exception failure) {
            try {
                job.getJobClient().get().cancel().get(30, TimeUnit.SECONDS);
            } catch (Exception cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    protected static Map<Long, String> writerThreads() {
        Map<Long, String> threads = new HashMap<>();
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            String name = thread.getName();
            if (thread.isAlive()
                    && (name.startsWith("maxcompute-sink-io-")
                            || name.startsWith("OdpsBucket writer thread")
                            || name.startsWith("OdpsBucket commit thread")))
                threads.put(thread.getId(), name);
        }
        return threads;
    }

    private void createSqlTable(
            TableEnvironment env,
            String sqlName,
            String physical,
            boolean sink,
            boolean upsert,
            String writer,
            int parallelism,
            Path temporary,
            int readTimeoutSeconds,
            String writerBufferSize,
            boolean dynamic,
            boolean grouped,
            boolean dynamicSourceAssignment) {
        String options =
                "'splitstrategy'='ROW_OFFSET','dynamicloadbalancing'='"
                        + dynamicSourceAssignment
                        + "','splitbatchreadsize'='64','sink.parallelism'='"
                        + parallelism
                        + "','sink.retry.delay'='10','sink.read.timeout'='"
                        + readTimeoutSeconds
                        + " s','insert.writer.buffer-size'='"
                        + writerBufferSize
                        + "','insert.arrow-writer.batch-size'='64'";
        if (dynamic)
            options +=
                    ",'sink.partition.default-value'='missing','sink.file-cached.writer.num'='"
                            + Integer.getInteger("emulator.cached.writer.threads", 1)
                            + "','insert.commit.thread-num'='"
                            + Integer.getInteger("emulator.cached.commit.threads", 2)
                            + "'";
        if (grouped) options += ",'sink.group-partition.enable'='true'";
        if (sink) {
            options += ",'sink.operation'='" + (upsert ? "upsert" : "insert") + "'";
            if (writer.equals("cached"))
                options +=
                        ",'sink.file-cached.enable'='true','sink.file-cached.rolling.max-size'='8"
                                + " kb','sink.file-cached.tmp.dirs'='"
                                + temporary.toString().replace("'", "''")
                                + "'";
            if (writer.equals("arrow")) options += ",'insert.arrow-writer.enable'='true'";
            if (writer.equals("modern")) options += ",'sink.coordinator.enable'='false'";
            if (writer.equals("stream-record"))
                options += ",'insert.streaming-tunnel.enable'='true'";
        }
        env.executeSql(
                "CREATE TABLE "
                        + sqlName
                        + "(id BIGINT,v BIGINT"
                        + (sink && upsert ? ",PRIMARY KEY(id) NOT ENFORCED" : "")
                        + (dynamic ? ",ds STRING,region STRING" : "")
                        + ")"
                        + (dynamic ? " PARTITIONED BY(ds,region)" : "")
                        + " WITH ('connector'='maxcompute','table.name'='test_project."
                        + physical
                        + "','odps.project.name'='test_project','odps.end.point'='"
                        + endpoint
                        + "','odps.tunnel.end.point'='"
                        + endpoint
                        + "','odps.access.id'='test-ak','odps.access.key'='test-sk',"
                        + options
                        + ")");
    }

    protected JsonObject control(String method, String path, String body) throws Exception {
        HttpURLConnection connection =
                (HttpURLConnection) new URL(endpoint + path).openConnection();
        connection.setRequestMethod(method);
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(10000);
        if (body != null) {
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type", "application/json");
            try (OutputStream output = connection.getOutputStream()) {
                output.write(body.getBytes(StandardCharsets.UTF_8));
            }
        }
        int status = connection.getResponseCode();
        try (InputStream input =
                status < 400 ? connection.getInputStream() : connection.getErrorStream()) {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            if (input != null) {
                byte[] buffer = new byte[4096];
                int n;
                while ((n = input.read(buffer)) != -1) bytes.write(buffer, 0, n);
            }
            String text = new String(bytes.toByteArray(), StandardCharsets.UTF_8);
            Assert.assertTrue(
                    "test control " + method + " " + path + ": " + status + " " + text,
                    status >= 200 && status < 300);
            return text.isEmpty()
                    ? new JsonObject()
                    : new JsonParser().parse(text).getAsJsonObject();
        } finally {
            connection.disconnect();
        }
    }
}
