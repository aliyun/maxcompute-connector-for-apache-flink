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
import com.google.gson.Gson;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.odps.table.OdpsOptions;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsPipeline;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Real chained Flink operator -> selected Sink -> SDK -> loopback Emulator. */
public class EmulatorInsertMailboxReproducer extends EmulatorWriteHarness {
    protected boolean fileCached() {
        return false;
    }

    protected boolean dynamic() {
        return Boolean.getBoolean("emulator.mailbox.dynamic");
    }

    protected int parallelism() {
        return Integer.getInteger("emulator.mailbox.parallelism", 1);
    }

    protected boolean modern() {
        return Boolean.getBoolean("emulator.mailbox.modern");
    }

    protected long checkpointIntervalMillis() {
        return 300;
    }

    protected long checkpointMinPauseMillis() {
        return 0;
    }

    protected int restartAttempts() {
        return 0;
    }

    protected boolean expectRecovery() {
        return false;
    }

    protected String reportLabel() {
        return modern() ? "modern-" : Boolean.getBoolean("emulator.mailbox.arrow") ? "arrow-" : "";
    }

    protected int completionTimeoutSeconds() {
        return Integer.getInteger("emulator.mailbox.completion.timeout-seconds", 15);
    }

    private static final int ROWS = Integer.getInteger("emulator.mailbox.rows", 10032);

    @Test(timeout = 90000)
    public void chainedBurstMustProcessBootstrapAckBeforeBufferStalls() throws Exception {
        runScenario(false);
    }

    @Test(timeout = 90000)
    public void checkpointRolloverMustProcessNewSessionAckDuringChainedBurst() throws Exception {
        runScenario(true);
    }

    private void runScenario(boolean rollover) throws Exception {
        String rule = table + "_bootstrap_delay";
        Map<String, Object> report = new LinkedHashMap<>();
        JobClient job = null;
        boolean finished = false;
        int expectedRows = parallelism() * (ROWS + (rollover ? 1 : 0));
        boolean recovering = rollover && expectRecovery();
        boolean injectDelay =
                !modern() && Integer.getInteger("emulator.mailbox.create-delay-ms", 300) > 0;
        long started = System.nanoTime();
        Path cache = Files.createTempDirectory("flink-mailbox-cache-");
        Set<Long> originalThreads = writerThreads().keySet();
        try {
            if (dynamic()) {
                sql("DROP TABLE " + table);
                sql("CREATE TABLE " + table + "(id BIGINT,v BIGINT) PARTITIONED BY(ds STRING)");
                // Isolate session rollover from the independent 5s new-partition batch timer.
                sql("ALTER TABLE " + table + " ADD PARTITION(ds='p0')");
            }
            if (injectDelay)
                control(
                        "PUT",
                        "/__test/faults/" + rule,
                        "{\"match\":{\"plane\":\"tunnel\",\"object\":\"upload\","
                                + "\"action\":\"create\",\"project\":\"test_project\",\"table\":\""
                                + table
                                + "\"},\"effect\":{\"type\":\"delay\",\"delay_ms\":"
                                + Integer.getInteger("emulator.mailbox.create-delay-ms", 300)
                                + "},"
                                + "\"times\":"
                                + (rollover ? 2 : 1)
                                + ",\"ttl_seconds\":120}");
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(parallelism());
            env.setRestartStrategy(
                    restartAttempts() == 0
                            ? RestartStrategies.noRestart()
                            : RestartStrategies.fixedDelayRestart(restartAttempts(), 100));
            RowType type =
                    dynamic()
                            ? RowType.of(
                                    new LogicalType[] {
                                        new BigIntType(),
                                        new BigIntType(),
                                        new org.apache.flink.table.types.logical.VarCharType()
                                    },
                                    new String[] {"id", "v", "ds"})
                            : RowType.of(
                                    new LogicalType[] {new BigIntType(), new BigIntType()},
                                    new String[] {"id", "v"});
            Configuration options = new Configuration();
            options.set(OdpsOptions.SINK_COORDINATOR_ENABLE, !modern());
            options.set(
                    OdpsOptions.INSERT_ARROW_WRITER_ENABLE,
                    Boolean.getBoolean("emulator.mailbox.arrow"));
            options.set(OdpsOptions.RETRY_DELAY_MILLIS, 10L);
            if (fileCached()) {
                options.set(OdpsOptions.SINK_FILE_CACHED_ENABLE, true);
                options.setString("sink.file-cached.rolling.max-size", "8 kb");
                options.setString("sink.file-cached.tmp.dirs", cache.toString());
                options.setString("sink.file-cached.writer.num", "1");
                options.setString("insert.commit.thread-num", "2");
            }
            org.apache.flink.streaming.api.datastream.DataStream<RowData> input;
            if (rollover) {
                env.enableCheckpointing(checkpointIntervalMillis());
                env.getCheckpointConfig().setCheckpointTimeout(10000);
                env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
                env.getCheckpointConfig().setMinPauseBetweenCheckpoints(checkpointMinPauseMillis());
                env.getCheckpointConfig().setTolerableCheckpointFailureNumber(9999);
                input =
                        env.fromSequence(0, 499L * parallelism())
                                .map(
                                        value -> {
                                            Thread.sleep(10);
                                            return value;
                                        })
                                .returns(org.apache.flink.api.common.typeinfo.Types.LONG)
                                .transform(
                                        "burst after first complete checkpoint",
                                        InternalTypeInfo.of(type),
                                        new CheckpointBurst(dynamic(), expectRecovery() ? checkpointIntervalMillis() + 150 : 0));
            } else {
                input =
                        env.fromSequence(0, parallelism() - 1L)
                                .rebalance()
                                .flatMap(new Burst(dynamic()))
                                .returns(InternalTypeInfo.of(type));
            }
            OdpsPipeline.builder()
                    .odpsConf(
                            new OdpsConf("test-ak", "test-sk", endpoint, "test_project", endpoint))
                    .tableName(table)
                    .configuration(options)
                    .sink(input, !rollover);
            job = env.executeAsync("insert mailbox bootstrap progress");
            try {
                JobExecutionResult result =
                        job.getJobExecutionResult()
                                .get(completionTimeoutSeconds(), TimeUnit.SECONDS);
                finished = true;
                report.put(
                        "job_elapsed_ms",
                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
                if (rollover) {
                    Integer checkpoints =
                            result.getAccumulatorResult("observed-complete-checkpoints");
                    report.put("completed_checkpoints", checkpoints);
                    assertNotNull("Checkpoint callback was not reached", checkpoints);
                    assertTrue(
                            "Need completed checkpoints around rollover or after restored rollover",
                            checkpoints >= (recovering ? 1 : 2) * parallelism());
                    Integer restored = result.getAccumulatorResult("mailbox-restored-subtasks");
                    report.put("restored_subtasks", restored == null ? 0 : restored);
                    if (recovering)
                        assertTrue(
                                "Expected actual checkpoint restoration",
                                restored != null && restored >= parallelism());
                }
            } catch (TimeoutException stalled) {
                report.put(
                        "job_state_at_timeout",
                        job.getJobStatus().get(5, TimeUnit.SECONDS).toString());
                List<String> stacks = new ArrayList<>();
                for (Map.Entry<Thread, StackTraceElement[]> entry :
                        Thread.getAllStackTraces().entrySet()) {
                    String stack = Arrays.toString(entry.getValue());
                    if (stack.contains("InsertFunction.processElement")) {
                        stacks.add(entry.getKey().getName() + ": " + stack);
                    }
                }
                report.put("sink_stacks", stacks);
                throw new AssertionError(
                        "Finite chained burst stalled waiting for bootstrap ACK", stalled);
            } catch (ExecutionException failure) {
                Throwable cause = failure;
                while (cause.getCause() != null) cause = cause.getCause();
                report.put("terminal_failure", cause.toString());
                throw failure;
            }
            TableTunnel tunnel = new TableTunnel(odps);
            tunnel.setEndpoint(endpoint);
            TableTunnel.DownloadSession download =
                    dynamic()
                            ? tunnel.createDownloadSession(
                                    "test_project",
                                    table,
                                    new com.aliyun.odps.PartitionSpec("ds=p0"))
                            : tunnel.createDownloadSession("test_project", table);
            long actualRows = download.getRecordCount();
            report.put("actual_rows", actualRows);
            if (recovering) {
                assertTrue("Recovery must preserve every row", actualRows >= expectedRows);
                assertTrue(
                        "Replay must be bounded by attempts",
                        actualRows <= expectedRows * (restartAttempts() + 1L));
            } else assertEquals("All input rows must be committed", expectedRows, actualRows);
            Set<Long> keys = new HashSet<>();
            try (TunnelRecordReader reader = download.openRecordReader(0, actualRows)) {
                com.aliyun.odps.data.Record row;
                while ((row = reader.read()) != null) {
                    long id = row.getBigint(0);
                    assertTrue(id >= 0 && id < expectedRows);
                    assertEquals(id * 7 + 3, row.getBigint(1).longValue());
                    boolean unique = keys.add(id);
                    if (!recovering) assertTrue("Duplicate key " + id, unique);
                }
            }
            assertEquals(expectedRows, keys.size());
            report.put("verified_rows", keys.size());
            report.put("duplicate_rows", actualRows - keys.size());
            if (injectDelay)
                assertEquals(
                        "Session delay must exercise each requested window",
                        rollover ? 2 : 1,
                        control("GET", "/__test/faults/" + rule, null).get("hits").getAsInt());
        } finally {
            report.put("rows", expectedRows);
            report.put("file_cached", fileCached());
            report.put("modern", modern());
            report.put("checkpoint_interval_ms", checkpointIntervalMillis());
            report.put("checkpoint_min_pause_ms", checkpointMinPauseMillis());
            report.put("restart_attempts", restartAttempts());
            report.put("dynamic_partition", dynamic());
            report.put("parallelism", parallelism());
            report.put("rollover", rollover);
            report.put("elapsed_ms", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
            report.put(
                    "create_fault_hits",
                    injectDelay
                            ? control("GET", "/__test/faults/" + rule, null).get("hits").getAsInt()
                            : 0);
            try {
                if (job != null && !finished && !job.getJobExecutionResult().isDone()) {
                    job.cancel().get(10, TimeUnit.SECONDS);
                }
            } finally {
                if (injectDelay) control("DELETE", "/__test/faults/" + rule, null);
                if (finished) {
                    Map<Long, String> remaining;
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    do {
                        remaining = writerThreads();
                        remaining.keySet().removeAll(originalThreads);
                        if (remaining.isEmpty()) break;
                        Thread.sleep(25);
                    } while (System.nanoTime() < deadline);
                    report.put("remaining_writer_threads", remaining.size());
                    assertTrue("Writer threads leaked: " + remaining, remaining.isEmpty());
                    try (java.util.stream.Stream<Path> paths = Files.walk(cache)) {
                        long files = paths.filter(Files::isRegularFile).count();
                        report.put("remaining_cache_files", files);
                        assertEquals("Spill files leaked", 0L, files);
                    }
                }
                try (java.util.stream.Stream<Path> paths = Files.walk(cache)) {
                    for (Path path :
                            (Iterable<Path>) paths.sorted(Comparator.reverseOrder())::iterator) {
                        Files.delete(path);
                    }
                }
                Path file =
                        Paths.get(
                                "target/mailbox-diagnostics/insert-mailbox-"
                                        + reportLabel()
                                        + (fileCached() ? "cached-" : "")
                                        + (rollover ? "rollover" : "bootstrap")
                                        + (dynamic() ? "-dynamic" : "-static")
                                        + "-p"
                                        + parallelism()
                                        + ".json");
                Files.createDirectories(file.getParent());
                Files.write(
                        file, (new Gson().toJson(report) + "\n").getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    /** A window-like operator emits a finite burst in a single mailbox invocation. */
    public static class Burst implements FlatMapFunction<Long, RowData> {
        private final boolean dynamic;

        public Burst(boolean dynamic) {
            this.dynamic = dynamic;
        }

        @Override
        public void flatMap(Long trigger, Collector<RowData> out) {
            for (long i = 0; i < ROWS; i++) out.collect(row(trigger * ROWS + i, dynamic));
        }
    }

    private static RowData row(long id, boolean dynamic) {
        return dynamic
                ? GenericRowData.of(
                        id, id * 7 + 3, org.apache.flink.table.data.StringData.fromString("p0"))
                : GenericRowData.of(id, id * 7 + 3);
    }

    /** A completed checkpoint arms the next ordinary input callback's window-like burst. */
    public static class CheckpointBurst extends AbstractStreamOperator<RowData>
            implements OneInputStreamOperator<Long, RowData> {
        private boolean emitted;
        private boolean seeded;
        private boolean ready;
        private boolean restored;
        private final boolean dynamic;
        private final long firstRolloverDelayMillis;
        private transient ListState<Integer> progress;

        public CheckpointBurst(boolean dynamic, long firstRolloverDelayMillis) {
            this.dynamic = dynamic;
            this.firstRolloverDelayMillis = firstRolloverDelayMillis;
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);
            progress =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "mailbox-burst-progress", Integer.class));
            if (context.isRestored()) {
                restored = true;
                for (int state : progress.get()) {
                    seeded = (state & 1) != 0;
                    emitted = (state & 2) != 0;
                }
                ready = true;
                getRuntimeContext().getIntCounter("mailbox-restored-subtasks").add(1);
            }
        }

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            super.snapshotState(context);
            progress.update(Collections.singletonList((seeded ? 1 : 0) | (emitted ? 2 : 0)));
        }

        @Override
        public void processElement(StreamRecord<Long> record) throws Exception {
            long offset = getRuntimeContext().getIndexOfThisSubtask() * (ROWS + 1L);
            if (!seeded) {
                seeded = true;
                output.collect(new StreamRecord<>(row(offset, dynamic)));
            }
            // Emit from a normal chained record callback while the input is active.
            // Emitting after endInput from a checkpoint notification is not valid.
            if (ready && !emitted) {
                emitted = true;
                // Model one slow window callback spanning the next checkpoint. The
                // restored attempt removes this one-off timing fault, not the data.
                if (!restored && firstRolloverDelayMillis > 0) {
                    Thread.sleep(firstRolloverDelayMillis);
                }
                for (long id = 1; id <= ROWS; id++) {
                    output.collect(new StreamRecord<>(row(offset + id, dynamic)));
                }
            }
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            super.notifyCheckpointComplete(checkpointId);
            getRuntimeContext().getIntCounter("observed-complete-checkpoints").add(1);
            ready = true;
        }
    }
}
