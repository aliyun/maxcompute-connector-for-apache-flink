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

import static org.junit.Assert.*;

import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** A real SQL task failure after an acknowledged checkpoint must restore and finish. */
@RunWith(Parameterized.class)
public class EmulatorSqlRecoveryITCase extends EmulatorWriteHarness {
    private final String writer;
    private final boolean append, dynamic, grouped;
    private final int parallelism, records;
    private final String scenario;

    @Parameterized.Parameters(name = "{0}/p={1}")
    public static Collection<Object[]> cases() {
        List<Object[]> cases = new ArrayList<>();
        for (String writer :
                Arrays.asList(
                        "record",
                        "cached",
                        "modern",
                        "append-record",
                        "append-cached",
                        "append-modern")) cases.add(new Object[] {writer, 2});
        for (String writer :
                Arrays.asList("append-cached-dynamic", "append-grouped-cached-dynamic"))
            for (int p : new int[] {4, 8, 16}) cases.add(new Object[] {writer, p});
        cases.removeIf(
                row ->
                        !(row[0] + "/" + row[1])
                                .matches(System.getProperty("emulator.recovery.filter", ".*")));
        if (cases.isEmpty()) throw new IllegalArgumentException("No recovery scenario matched");
        return cases;
    }

    public EmulatorSqlRecoveryITCase(String writer, int parallelism) {
        super();
        this.append = writer.startsWith("append-");
        this.dynamic = writer.contains("dynamic");
        this.grouped = writer.contains("grouped-");
        this.writer = writer.replace("append-", "").replace("grouped-", "").replace("-dynamic", "");
        this.parallelism = parallelism;
        this.records = dynamic ? 1024 : 160;
        this.scenario = "recovery_" + writer + "_p" + parallelism;
    }

    @Test(timeout = 240000)
    public void sqlRestartRestoresSourceAndSinkWithoutMissingKeys() throws Exception {
        sql("DROP TABLE " + table);
        sql(
                "CREATE TABLE "
                        + table
                        + (append
                                ? "(id BIGINT,v BIGINT)"
                                : "(id BIGINT,v BIGINT,PRIMARY KEY(id))"
                                      + " TBLPROPERTIES('transactional'='true','write.bucket.num'='1')")
                        + (dynamic ? " PARTITIONED BY(ds STRING,region STRING)" : ""));
        Path evidence = Files.createTempDirectory("flink-odps-recovery-");
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("scenario", scenario);
        report.put("writer", writer);
        report.put("parallelism", parallelism);
        report.put("dynamic_partitions", dynamic);
        report.put("group_partitions", grouped);
        if (dynamic) {
            report.put(
                    "cached_writer_threads",
                    Integer.getInteger("emulator.cached.writer.threads", 1));
            report.put(
                    "cached_commit_threads",
                    Integer.getInteger("emulator.cached.commit.threads", 2));
        }
        report.put("rows_per_output", records);
        report.put("upsert", !append);
        report.put("outputs", 1);
        Set<Long> originalThreads = writerThreads().keySet();
        String delayRule = table + "_restore_delay";
        Throwable primaryFailure = null;
        long started = System.nanoTime();
        try {
            if (dynamic) {
                control("POST", "/__test/write-metrics", "{}");
                control(
                        "PUT",
                        "/__test/faults/" + delayRule,
                        "{\"match\":{\"plane\":\"tunnel\",\"object\":\"upload\",\"action\":\"write\",\"project\":\"test_project\",\"table\":\""
                                + table
                                + "\"},\"effect\":{\"type\":\"delay\",\"delay_ms\":30},\"times\":10000,\"ttl_seconds\":600}");
            }
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(dynamic ? parallelism : 1);
            env.enableCheckpointing(300);
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 50));
            env.getCheckpointConfig().setCheckpointTimeout(60000);
            env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
            // A cached checkpoint uploads every partition. Leave a quiet interval after it
            // completes instead of immediately triggering another barrier while tasks close.
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
            env.getCheckpointConfig()
                    .setCheckpointStorage(evidence.resolve("checkpoints").toUri().toString());
            StreamTableEnvironment sqlEnv = StreamTableEnvironment.create(env);
            org.apache.flink.api.java.typeutils.RowTypeInfo type =
                    new org.apache.flink.api.java.typeutils.RowTypeInfo(
                            new org.apache.flink.api.common.typeinfo.TypeInformation<?>[] {
                                org.apache.flink.api.common.typeinfo.Types.LONG,
                                org.apache.flink.api.common.typeinfo.Types.LONG
                            },
                            new String[] {"id", "v"});
            sqlEnv.createTemporaryView(
                    "input_rows",
                    sqlEnv.fromDataStream(
                            env.addSource(new RestorableRows(evidence.toString(), records), type)
                                    .setParallelism(1)
                                    .uid("restorable-sql-fixture"),
                            Schema.newBuilder()
                                    .column("id", DataTypes.BIGINT())
                                    .column("v", DataTypes.BIGINT())
                                    .build()));
            sqlEnv.createTemporarySystemFunction(
                    "fail_once",
                    new FailOnce(evidence.toString(), dynamic ? records / 2 : append ? 80 : 100));
            String options =
                    "'sink.operation'='"
                            + (append ? "insert" : "upsert")
                            + "','sink.parallelism'='"
                            + parallelism
                            + "','sink.retry.delay'='10'";
            if (writer.equals("cached"))
                options +=
                        ",'sink.file-cached.enable'='true','sink.file-cached.rolling.max-size'='8"
                                + " kb','sink.file-cached.tmp.dirs'='"
                                + evidence.resolve("cache")
                                + "'";
            if (dynamic)
                options +=
                        ",'sink.partition.default-value'='missing','sink.file-cached.writer.num'='"
                                + Integer.getInteger("emulator.cached.writer.threads", 1)
                                + "','insert.commit.thread-num'='"
                                + Integer.getInteger("emulator.cached.commit.threads", 2)
                                + "'";
            if (grouped) options += ",'sink.group-partition.enable'='true'";
            if (writer.equals("modern"))
                options +=
                        ",'sink.coordinator.enable'='false','insert.arrow-writer.batch-size'='16'";
            sqlEnv.executeSql(
                    "CREATE TABLE restored_rows(id BIGINT,v BIGINT"
                            + (append ? "" : ",PRIMARY KEY(id) NOT ENFORCED")
                            + (dynamic ? ",ds STRING,region STRING" : "")
                            + ")"
                            + (dynamic ? " PARTITIONED BY(ds,region)" : "")
                            + " WITH ('connector'='maxcompute','table.name'='test_project."
                            + table
                            + "','odps.project.name'='test_project','odps.end.point'='"
                            + endpoint
                            + "','odps.tunnel.end.point'='"
                            + endpoint
                            + "','odps.access.id'='test-ak','odps.access.key'='test-sk',"
                            + options
                            + ")");
            TableResult job =
                    sqlEnv.executeSql(
                            "INSERT INTO restored_rows SELECT id,fail_once(id,v)"
                                    + (dynamic ? "," + CachedDynamicPartitions.sqlColumns() : "")
                                    + " FROM input_rows");
            try {
                job.await(180, TimeUnit.SECONDS);
            } catch (Exception failure) {
                try {
                    job.getJobClient().get().cancel().get(30, TimeUnit.SECONDS);
                } catch (Exception cleanup) {
                    failure.addSuppressed(cleanup);
                }
                throw failure;
            }
            assertTrue(
                    "no checkpoint completed",
                    Files.exists(evidence.resolve("checkpoint-complete")));
            assertTrue("failure was not injected", Files.exists(evidence.resolve("failed-once")));
            assertTrue("source did not restore", Files.exists(evidence.resolve("source-restored")));
            TableTunnel tunnel = new TableTunnel(odps);
            tunnel.setEndpoint(endpoint);
            long count;
            if (dynamic) {
                count = CachedDynamicPartitions.verify(odps, tunnel, table, records, false, false);
                report.put(
                        "verified_partitions_per_output",
                        CachedDynamicPartitions.partitionCount(records));
            } else {
                TableTunnel.DownloadSession session =
                        tunnel.createDownloadSession("test_project", table);
                count = session.getRecordCount();
                boolean replayMayDuplicate = append;
                if (replayMayDuplicate) {
                    assertTrue(session.getRecordCount() >= 160 && session.getRecordCount() <= 320);
                } else {
                    assertEquals(160L, session.getRecordCount());
                }
                Set<Long> ids = new HashSet<>();
                try (TunnelRecordReader reader =
                        session.openRecordReader(0, session.getRecordCount())) {
                    com.aliyun.odps.data.Record record;
                    while ((record = reader.read()) != null) {
                        long id = record.getBigint(0);
                        assertTrue("unexpected id " + id, id >= 0 && id < 160);
                        if (replayMayDuplicate) ids.add(id);
                        else assertTrue("duplicate key", ids.add(id));
                        assertEquals(id * 7 + 3, record.getBigint(1).longValue());
                    }
                }
                for (long id = 0; id < 160; id++) assertTrue("missing key " + id, ids.contains(id));
            }
            if (writer.equals("cached") && Files.exists(evidence.resolve("cache"))) {
                try (java.util.stream.Stream<Path> cache = Files.walk(evidence.resolve("cache"))) {
                    assertEquals(
                            "file cache leaked after recovery",
                            0L,
                            cache.filter(Files::isRegularFile).count());
                }
            }
            if (dynamic) {
                com.google.gson.JsonObject metrics = control("GET", "/__test/write-metrics", null);
                assertEquals("write requests leaked", 0, metrics.get("active").getAsInt());
                assertTrue(
                        "no concurrent recovered data uploads",
                        metrics.get("peak_data").getAsInt() >= 2);
                int hits =
                        control("GET", "/__test/faults/" + delayRule, null).get("hits").getAsInt();
                assertTrue("restore backpressure did not fire", hits > 0);
                report.put("peak_data_writes", metrics.get("peak_data").getAsInt());
                report.put("fault_hits", Collections.singletonList(hits));
            }
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            Map<Long, String> remaining;
            do {
                remaining = writerThreads();
                remaining.keySet().removeAll(originalThreads);
                if (remaining.isEmpty()) break;
                Thread.sleep(25);
            } while (System.nanoTime() < deadline);
            assertTrue("writer threads leaked " + remaining, remaining.isEmpty());
            report.put("remaining_writer_threads", remaining.size());
            report.put("checkpoint_completed", true);
            report.put("task_failure_injected", true);
            report.put("source_restored", true);
            int restoredOffset =
                    Integer.parseInt(
                            new String(
                                    Files.readAllBytes(evidence.resolve("source-restored")),
                                    java.nio.charset.StandardCharsets.UTF_8));
            if (dynamic)
                assertTrue(
                        "restore did not resume nonzero persisted progress",
                        restoredOffset > 0 && restoredOffset < records);
            report.put("restored_source_offset", restoredOffset);
            report.put("record_counts", Collections.singletonList(count));
            report.put("duplicate_counts", Collections.singletonList(count - records));
            report.put("cache_files_remaining", 0);
            report.put("status", "PASS");
        } catch (Throwable failure) {
            primaryFailure = failure;
            report.put("status", "FAIL");
            report.put("error", failure.toString());
            throw failure;
        } finally {
            List<Throwable> cleanupFailures = new ArrayList<>();
            if (dynamic) {
                try {
                    control("DELETE", "/__test/faults/" + delayRule, null);
                } catch (Throwable failure) {
                    cleanupFailures.add(failure);
                }
            }
            try (java.util.stream.Stream<Path> files = Files.walk(evidence)) {
                for (Path path :
                        files.sorted(Comparator.reverseOrder())
                                .collect(java.util.stream.Collectors.toList())) {
                    try {
                        Files.deleteIfExists(path);
                    } catch (Throwable failure) {
                        cleanupFailures.add(failure);
                    }
                }
            } catch (Throwable failure) {
                cleanupFailures.add(failure);
            }
            report.put(
                    "cleanup_failures",
                    cleanupFailures.stream()
                            .map(Throwable::toString)
                            .collect(java.util.stream.Collectors.toList()));
            if (!cleanupFailures.isEmpty()) report.put("status", "FAIL");
            report.put("duration_ms", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
            try {
                Path reportPath = Paths.get("target/readiness", scenario + ".json");
                Files.createDirectories(reportPath.getParent());
                Files.write(
                        reportPath,
                        new com.google.gson.GsonBuilder()
                                .setPrettyPrinting()
                                .create()
                                .toJson(report)
                                .getBytes(java.nio.charset.StandardCharsets.UTF_8));
            } catch (Throwable failure) {
                cleanupFailures.add(failure);
            }
            if (!cleanupFailures.isEmpty()) {
                if (primaryFailure != null)
                    for (Throwable failure : cleanupFailures) primaryFailure.addSuppressed(failure);
                else {
                    AssertionError failure =
                            new AssertionError("Recovery cleanup failed " + scenario);
                    for (Throwable cleanupFailure : cleanupFailures)
                        failure.addSuppressed(cleanupFailure);
                    throw failure;
                }
            }
        }
    }

    public static final class FailOnce extends ScalarFunction {
        private final String directory;
        private final int threshold;

        public FailOnce(String directory, int threshold) {
            this.directory = directory;
            this.threshold = threshold;
        }

        @Override
        public boolean isDeterministic() {
            return false;
        }

        public Long eval(Long id, Long value) {
            if (id >= threshold && Files.exists(Paths.get(directory, "checkpoint-complete"))) {
                try {
                    Files.createFile(Paths.get(directory, "failed-once"));
                } catch (FileAlreadyExistsException alreadyFailed) {
                    return value;
                } catch (java.io.IOException e) {
                    throw new RuntimeException(e);
                }
                throw new RuntimeException("injected SQL failure after checkpoint");
            }
            return value;
        }
    }

    public static final class RestorableRows extends RichParallelSourceFunction<Row>
            implements CheckpointedFunction, CheckpointListener {
        private final String directory;
        private transient ListState<Integer> state;
        private int next;
        private final int records;
        private volatile boolean running = true;

        public RestorableRows(String directory, int records) {
            this.directory = directory;
            this.records = records;
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            state =
                    context.getOperatorStateStore()
                            .getListState(new ListStateDescriptor<>("next-row", Integer.class));
            if (context.isRestored()) {
                for (int value : state.get()) next = value;
                Files.write(
                        Paths.get(directory, "source-restored"),
                        Integer.toString(next).getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            state.clear();
            state.add(next);
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            Files.write(
                    Paths.get(directory, "checkpoint-complete"),
                    Long.toString(checkpointId).getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }

        @Override
        public void run(SourceContext<Row> context) throws Exception {
            while (running && next < records) {
                if (next == records / 2
                        && !Files.exists(Paths.get(directory, "checkpoint-complete"))) {
                    Thread.sleep(10);
                    continue;
                }
                synchronized (context.getCheckpointLock()) {
                    context.collect(Row.of((long) next, next * 7L + 3));
                    next++;
                }
                Thread.sleep(5);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
