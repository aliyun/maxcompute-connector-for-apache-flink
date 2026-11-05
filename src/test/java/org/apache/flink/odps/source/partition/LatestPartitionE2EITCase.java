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
 * Modified for the MaxCompute Flink connector; see NOTICE for upstream attribution.
 */

package org.apache.flink.odps.source.partition;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.CompressionCodec;
import com.aliyun.odps.table.configuration.IncrementalOptions;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.type.TypeInfoFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.odps.schema.OdpsColumn;
import org.apache.flink.odps.source.ContinuousOdpsSource;
import org.apache.flink.odps.source.OdpsSplitStrategy;
import org.apache.flink.odps.source.OdpsStartupMode;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.odps.test.util.MaxComputeTestEnvironment;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.*;

/**
 * Explicit opt-in, real MaxCompute + local Flink MiniCluster E2E. Never run on customer tables.
 * Execute with -Dtest=LatestPartitionE2EITCase -Dmaxcompute.e2e.allow-write=true and broker env.
 * The source, enumerator, reader, session planner and checkpoint machinery are all production code.
 */
public class LatestPartitionE2EITCase {
    @Test(timeout = 480000)
    public void deltaSwitchAndCheckpointRecovery() throws Exception { runScenario(false); }

    @Test(timeout = 480000)
    public void dlmvSwitchAndCheckpointRecovery() throws Exception { runScenario(true); }

    private void runScenario(boolean materializedView) throws Exception {
        assertEquals("Explicit isolated-table write authorization required", "true",
                System.getProperty("maxcompute.e2e.allow-write"));
        assertEquals("Run this Flink 1.17 E2E with JDK 17", "17", System.getProperty("java.specification.version"));
        OdpsConf conf = MaxComputeTestEnvironment.getOdpsConf();
        String project = conf.getProject();
        conf.setProperty("scan.partition.latest", "true");
        conf.setProperty("scan.partition.ready-mode", "partition-visible");
        Odps odps = OdpsUtils.getOdps(conf);
        odps.setCurrentSchema("default");
        String prefix = "flink_latest_e2e_" + UUID.randomUUID().toString().replace("-", "");
        String base = prefix + "_base", mv = prefix + "_mv";
        String target = materializedView ? mv : base;
        String qualifiedBase = project + ".default." + base;
        String qualifiedMv = project + ".default." + mv;
        Path directory = Files.createTempDirectory("flink-latest-e2e-");
        JobClient job = null;
        boolean createAttempted = false;
        assertFalse(odps.tables().exists(project, "default", base));
        assertFalse(odps.tables().exists(project, "default", mv));
        try {
            createAttempted = true;
            sql(odps, "create table " + qualifiedBase
                    + "(id bigint not null primary key,amount bigint) partitioned by(pt string) "
                    + "tblproperties('transactional'='true','acid.cdc.mode.enable'='true',"
                    + "'acid.cdc.build.async'='false','cdc.insert.into.passthrough.enable'='true',"
                    + "'write.bucket.num'='2') lifecycle 1;");
            insert(odps, qualifiedBase, "20260910", 1);
            if (materializedView) {
                sql(odps, "create materialized view " + qualifiedMv + " primary key(id) build deferred "
                        + "partitioned on(pt) tblproperties('refresh_mode'='incremental',"
                        + "'enable_auto_refresh'='false','acid.cdc.mode.enable'='true',"
                        + "'acid.cdc.build.async'='false','write.bucket.num'='2') "
                        + "as select id,amount,pt from " + qualifiedBase + ";");
                refresh(odps, qualifiedMv, "20260910");
            }
            Configuration config = new Configuration();
            config.setString("rest.bind-port", "0");
            config.setString("heartbeat.timeout", "60000");
            StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2, config);
            env.getConfig().disableClosureCleaner();
            env.enableCheckpointing(500);
            env.getCheckpointConfig().setCheckpointTimeout(60000);
            env.getCheckpointConfig().setCheckpointStorage(directory.resolve("checkpoints").toUri().toString());
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 1000));
            RowType rowType = RowType.of(new BigIntType(), new BigIntType(), new VarCharType());
            ContinuousOdpsSource source = new ContinuousOdpsSource(conf, true,
                    TableIdentifier.of(project, "default", target), Collections.emptyList(),
                    new OdpsColumn[]{new OdpsColumn("id", TypeInfoFactory.BIGINT),
                            new OdpsColumn("amount", TypeInfoFactory.BIGINT)},
                    new OdpsColumn[]{new OdpsColumn("pt", TypeInfoFactory.STRING, true)},
                    InternalTypeInfo.of(rowType), 128, CompressionCodec.NO_COMPRESSION,
                    OdpsSplitStrategy.BUCKET, 0, 10, 2000, OdpsStartupMode.FROM_SNAPSHOT,
                    IncrementalOptions.newBuilder().startVersion(1L)
                            .withIncrementalMode(IncrementalOptions.IncrementalMode.CDC).build(), ZoneId.of("UTC"));
            env.fromSource(source, WatermarkStrategy.noWatermarks(), "latest-partition")
                    .uid("latest-partition").setParallelism(2)
                    .addSink(new CheckpointResultSink(directory.toString()))
                    .uid("checkpoint-results").setParallelism(1);
            job = env.executeAsync("latest-partition-e2e-" + (materializedView ? "mv" : "delta"));
            report("E2E job=" + job.getJobID() + " table=" + target);
            awaitRows(job, directory, expected(1));

            publish(odps, qualifiedBase, qualifiedMv, materializedView, "20260911", 2);
            awaitRows(job, directory, expected(1, 2));
            // A late old-partition write must not leak through the new partition's sessions.
            publish(odps, qualifiedBase, qualifiedMv, materializedView, "20260910", 99);
            publish(odps, qualifiedBase, qualifiedMv, materializedView, "20260911", 3);
            awaitRows(job, directory, expected(1, 2, 3));

            // The result above is already checkpoint-committed. Crash on the next arriving record.
            Files.write(directory.resolve("fail-next-record"), new byte[]{1});
            publish(odps, qualifiedBase, qualifiedMv, materializedView, "20260911", 4);
            awaitRows(job, directory, expected(1, 2, 3, 4));
            assertTrue("The fault must actually execute", Files.exists(directory.resolve("fault-fired")));
            assertTrue("Flink must restore operator state", Files.exists(directory.resolve("restored")));

            publish(odps, qualifiedBase, qualifiedMv, materializedView, "20260912", 5);
            awaitRows(job, directory, expected(1, 2, 3, 4, 5));
            publish(odps, qualifiedBase, qualifiedMv, materializedView, "20260911", 98);
            publish(odps, qualifiedBase, qualifiedMv, materializedView, "20260912", 6);
            awaitRows(job, directory, expected(1, 2, 3, 4, 5, 6));
            report("E2E PASS " + (materializedView ? "DLMV" : "DELTA")
                    + " checkpointed_rows=" + Files.readAllLines(directory.resolve("committed"))
                    + " restored=" + Files.readAllLines(directory.resolve("restored")));
        } finally {
            try {
                if (job != null) { job.cancel().get(60, TimeUnit.SECONDS); }
            } finally {
                try {
                    if (createAttempted && odps.tables().exists(project, "default", mv)) {
                        sql(odps, "drop materialized view " + qualifiedMv + ";");
                    }
                } finally {
                    try {
                        if (createAttempted && odps.tables().exists(project, "default", base)) {
                            sql(odps, "drop table " + qualifiedBase + ";");
                        }
                        assertFalse(odps.tables().exists(project, "default", mv));
                        assertFalse(odps.tables().exists(project, "default", base));
                        report("E2E CLEANUP both_objects_absent " + prefix);
                    } finally {
                        // Checkpoints/job state may contain credentials. Never keep them as evidence.
                        try (Stream<Path> paths = Files.walk(directory)) {
                            for (Path path : paths.sorted(Comparator.reverseOrder()).collect(Collectors.toList())) {
                                Files.deleteIfExists(path);
                            }
                        }
                    }
                }
            }
        }
    }

    private static void report(String message) throws Exception {
        System.out.println(message);
        String progress = System.getProperty("maxcompute.e2e.progress");
        if (progress != null) {
            Files.write(Paths.get(progress), Collections.singletonList(message), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
    }

    private static void sql(Odps odps, String statement) throws Exception {
        Map<String, String> hints = new HashMap<>();
        hints.put("odps.namespace.schema", "true"); hints.put("odps.sql.type.system.odps2", "true");
        Instance instance = SQLTask.run(odps, odps.getDefaultProject(), statement, hints, null);
        report("E2E SQL " + instance.getId());
        instance.waitForSuccess();
    }

    private static void insert(Odps odps, String base, String partition, long id) throws Exception {
        sql(odps, "insert into " + base + " partition(pt='" + partition + "') values(" + id + "," + id * 100 + ");");
    }

    private static void refresh(Odps odps, String mv, String partition) throws Exception {
        sql(odps, "alter materialized view " + mv + " rebuild partition(pt='" + partition + "');");
    }

    private static void publish(Odps odps, String base, String mv, boolean refresh, String partition, long id) throws Exception {
        insert(odps, base, partition, id);
        if (refresh) { refresh(odps, mv, partition); }
    }

    private static List<String> expected(int... ids) {
        List<String> rows = new ArrayList<>();
        for (int id : ids) {
            String pt = id == 1 ? "20260910" : id <= 4 ? "20260911" : "20260912";
            rows.add("INSERT:" + id + ":" + id * 100 + ":" + pt);
        }
        Collections.sort(rows);
        return rows;
    }

    private static void awaitRows(JobClient job, Path directory, List<String> expected) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
        List<String> actual = Collections.emptyList();
        while (System.nanoTime() < deadline) {
            if (Files.exists(directory.resolve("committed"))) {
                actual = Files.readAllLines(directory.resolve("committed"));
                Collections.sort(actual);
                if (actual.equals(expected)) { report("E2E CHECKPOINT " + actual); return; }
                // Unexpected/duplicate committed rows cannot be repaired by waiting longer.
                assertTrue("Unexpected checkpoint result: " + actual, expected.containsAll(actual));
                assertEquals("Duplicate checkpoint result", actual.size(), new java.util.HashSet<>(actual).size());
            }
            if (job.getJobStatus().get(10, TimeUnit.SECONDS).isGloballyTerminalState()) {
                job.getJobExecutionResult().get(10, TimeUnit.SECONDS);
                fail("Job terminated before expected output");
            }
            Thread.sleep(100);
        }
        fail("Timed out waiting for checkpointed rows " + expected + "; actual=" + actual);
    }

    /** State is the sink's result, not an external append log that could hide replay duplicates. */
    public static final class CheckpointResultSink extends RichSinkFunction<RowData>
            implements CheckpointedFunction, CheckpointListener {
        private final String directory;
        private transient ListState<String> state;
        private final List<String> rows = new ArrayList<>();
        private transient TreeMap<Long, List<String>> snapshots;
        CheckpointResultSink(String directory) { this.directory = directory; }

        @Override public void initializeState(FunctionInitializationContext context) throws Exception {
            state = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("rows", Types.STRING));
            snapshots = new TreeMap<>();
            if (context.isRestored()) {
                for (String row : state.get()) { rows.add(row); }
                Files.write(Paths.get(directory, "restored"), Collections.singletonList("rows=" + rows.size()),
                        StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            }
        }
        @Override public void invoke(RowData row, Context context) throws Exception {
            Path trigger = Paths.get(directory, "fail-next-record");
            if (Files.exists(trigger)) {
                Files.move(trigger, Paths.get(directory, "fault-fired"));
                throw new RuntimeException("Intentional E2E failure after a completed checkpoint");
            }
            rows.add(row.getRowKind().name() + ":" + row.getLong(0) + ":" + row.getLong(1) + ":" + row.getString(2));
        }
        @Override public void snapshotState(FunctionSnapshotContext context) throws Exception {
            state.update(rows);
            snapshots.put(context.getCheckpointId(), new ArrayList<>(rows));
        }
        @Override public void notifyCheckpointComplete(long checkpointId) throws Exception {
            List<String> committed = snapshots.get(checkpointId);
            if (committed != null) {
                Path temporary = Paths.get(directory, "committed.tmp");
                Files.write(temporary, committed, StandardCharsets.UTF_8);
                Files.move(temporary, Paths.get(directory, "committed"), StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
                snapshots.headMap(checkpointId, true).clear();
            }
        }
    }
}
