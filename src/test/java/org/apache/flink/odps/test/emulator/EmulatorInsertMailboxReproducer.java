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

/** Real chained Flink operator -> coordinated Sink -> SDK -> loopback Emulator. */
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
            control(
                    "PUT",
                    "/__test/faults/" + rule,
                    "{\"match\":{\"plane\":\"tunnel\",\"object\":\"upload\","
                            + "\"action\":\"create\",\"project\":\"test_project\",\"table\":\""
                            + table
                            + "\"},\"effect\":{\"type\":\"delay\",\"delay_ms\":300},"
                            + "\"times\":"
                            + (rollover ? 2 : 1)
                            + ",\"ttl_seconds\":120}");
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(parallelism());
            env.setRestartStrategy(
                    org.apache.flink.api.common.restartstrategy.RestartStrategies.noRestart());
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
            options.set(OdpsOptions.INSERT_ARROW_WRITER_ENABLE, false);
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
                env.enableCheckpointing(300);
                env.getCheckpointConfig().setCheckpointTimeout(10000);
                env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
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
                                        new CheckpointBurst(dynamic()));
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
                                .get(
                                        Integer.getInteger(
                                                "emulator.mailbox.completion.timeout-seconds", 15),
                                        TimeUnit.SECONDS);
                finished = true;
                if (rollover) {
                    Integer checkpoints =
                            result.getAccumulatorResult("observed-complete-checkpoints");
                    report.put("completed_checkpoints", checkpoints);
                    assertNotNull("Checkpoint callback was not reached", checkpoints);
                    assertTrue(
                            "Need a checkpoint before and after rollover on every subtask",
                            checkpoints >= 2 * parallelism());
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
            assertEquals(
                    "All input rows must be committed", expectedRows, download.getRecordCount());
            Set<Long> keys = new HashSet<>();
            try (TunnelRecordReader reader = download.openRecordReader(0, expectedRows)) {
                com.aliyun.odps.data.Record row;
                while ((row = reader.read()) != null) {
                    long id = row.getBigint(0);
                    assertTrue(id >= 0 && id < expectedRows);
                    assertEquals(id * 7 + 3, row.getBigint(1).longValue());
                    assertTrue("Duplicate key " + id, keys.add(id));
                }
            }
            assertEquals(expectedRows, keys.size());
            report.put("verified_rows", keys.size());
            assertEquals(
                    "Session delay must exercise each requested window",
                    rollover ? 2 : 1,
                    control("GET", "/__test/faults/" + rule, null).get("hits").getAsInt());
        } finally {
            report.put("rows", expectedRows);
            report.put("file_cached", fileCached());
            report.put("dynamic_partition", dynamic());
            report.put("parallelism", parallelism());
            report.put("rollover", rollover);
            report.put("elapsed_ms", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
            report.put(
                    "create_fault_hits",
                    control("GET", "/__test/faults/" + rule, null).get("hits").getAsInt());
            try {
                if (job != null && !finished && !job.getJobExecutionResult().isDone()) {
                    job.cancel().get(10, TimeUnit.SECONDS);
                }
            } finally {
                control("DELETE", "/__test/faults/" + rule, null);
                if (fileCached() && finished) {
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
        private final boolean dynamic;

        public CheckpointBurst(boolean dynamic) {
            this.dynamic = dynamic;
        }

        @Override
        public void processElement(StreamRecord<Long> record) {
            long offset = getRuntimeContext().getIndexOfThisSubtask() * (ROWS + 1L);
            if (!seeded) {
                seeded = true;
                output.collect(new StreamRecord<>(row(offset, dynamic)));
            }
            // Emit from a normal chained record callback while the input is active.
            // Emitting after endInput from a checkpoint notification is not valid.
            if (ready && !emitted) {
                emitted = true;
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
