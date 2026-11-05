/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for the regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

package org.apache.flink.odps.test.shuffle;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.odps.test.util.OdpsTestUtils;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import com.aliyun.odps.Odps;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * FM-14: limited-scale fault recovery of the dynamic shuffle sink against a real MaxCompute
 * service (plan 用例 D with 用例 A/C acceptance on real data volume and primary keys).
 *
 * <p>Topology is the production one, installed by {@code OdpsDynamicTableSink}:
 * <pre>
 *   bounded replayable source → StatisticsOperator(+ShuffleCoordinator) → partitionCustom → upsert sink
 * </pre>
 * A task failure is injected while the job runs; the source is not checkpointable, so Flink's
 * restart replays the whole stream and the run becomes an at-least-once test: after recovery the
 * primary-key table must hold exactly one row per id (idempotent upsert), all ids, no duplicates.
 *
 * <p>Verification goes through MaxCompute SQL, not {@code Table#read(long)}: streaming
 * (WriteMode.STREAMING_REALTIME) rows are visible to SQL but not to a batch tunnel read, which is
 * why the pre-existing {@code MaxComputeSinkSystemTest} counted 0 for rows the SDK had already
 * confirmed writing (measured 2026-10-02: probe counted 100/100 by SQL for the same table).
 *
 * <p>Opt-in only, mirroring {@code LatestPartitionE2EITCase}'s discipline: it needs the
 * connector's own test identity, an explicit write authorization, and JDK 17.
 * <pre>
 * mvn -Pmaxcompute-integration verify -Dit.test=DynamicShuffleSinkRecoverySystemTest \
 *     -Dmaxcompute.e2e.allow-write=true
 * </pre>
 * The table is uniquely named with {@code lifecycle 1} and dropped in {@code @AfterClass}.
 */
public class DynamicShuffleSinkRecoverySystemTest {

    private static final int NUM_ROWS = 20_000;
    private static final int PARTITIONS = 4;
    private static final int PARALLELISM = 2;
    private static final int REWRITE_KEYS = 5_000;
    private static final long REWRITE_SCORE_BASE = 1_000_000L;
    /** Wall-clock budget for one SQL readback: scheduling on the test cluster takes ~1 min. */
    private static final long SQL_DEADLINE_MS = 300_000L;

    private static final AtomicBoolean FAILURE_FIRED = new AtomicBoolean(false);
    private static final AtomicInteger SOURCE_OPENS = new AtomicInteger(0);
    /** highest sink-subtask routing seen, so the log can show the assignment actually spread. */
    private static final Set<Integer> ROUTED_SUBTASKS = new HashSet<>();

    private static OdpsConf odpsConf;
    private static Odps odps;
    private static String table;
    private static int threadsBefore;
    private static Set<String> tmpFilesBefore;
    private static String originalTmpDir;
    private static File runTmpDir;

    @BeforeClass
    public static void setUp() throws Exception {
        Assert.assertEquals("Explicit isolated-table write authorization required", "true",
                System.getProperty("maxcompute.e2e.allow-write"));
        Assert.assertEquals("Run this E2E with JDK 17 (Flink 1.17 + Arrow need it)", "17",
                System.getProperty("java.specification.version"));

        odpsConf = OdpsTestUtils.getOdpsConf();
        Assert.assertNull("FM14 currently requires AK/SK; its SQL table fixture does not propagate STS tokens",
                odpsConf.getProperty("odps.access.security.token"));
        odps = OdpsUtils.getOdps(odpsConf);
        String candidate = "flink_dynshuffle_rcv_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        Assert.assertFalse("isolated table must not pre-exist", odps.tables().exists(odpsConf.getProject(), candidate));
        table = candidate;

        originalTmpDir = System.getProperty("java.io.tmpdir");
        runTmpDir = Files.createTempDirectory("flink-fm14-").toFile();
        System.setProperty("java.io.tmpdir", runTmpDir.getAbsolutePath());
        threadsBefore = Thread.getAllStackTraces().size();
        tmpFilesBefore = listingOf(runTmpDir.getAbsolutePath());
        execSql("create table " + table + "(\n"
                + "  id bigint not null,\n"
                + "  score bigint,\n"
                + "  primary key(id)\n"
                + ") partitioned by (ds string)\n"
                + "tblproperties('transactional'='true','write.bucket.num'='16') lifecycle 1;");

        System.out.printf("[fm14] table=%s project=%s threadsBefore=%d tmpFilesBefore=%d%n",
                table, odpsConf.getProject(), threadsBefore, tmpFilesBefore.size());
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (table == null) {
            return;
        }
        try {
            execSql("drop table if exists " + table + ";");
            boolean gone = !odps.tables().exists(odpsConf.getProject(), table);
            int threadsAfter = Thread.getAllStackTraces().size();
            // MiniCluster shutdown completes asynchronously after job termination. Inspect only
            // this run's directory; concurrent Maven processes may use the system temp directory.
            if (runTmpDir == null) {
                Assert.assertTrue("the isolated table must be dropped after setup failure", gone);
                return;
            }
            Set<String> leaked;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            do {
                leaked = listingOf(runTmpDir.getAbsolutePath());
                leaked.removeAll(tmpFilesBefore);
                if (leaked.isEmpty()) {
                    break;
                }
                Thread.sleep(200L);
            } while (System.nanoTime() < deadline);
            System.out.printf("[fm14] cleanup tableGone=%b threadsAfter=%d (before %d) newTmpFiles=%s%n",
                    gone, threadsAfter, threadsBefore, leaked);
            Assert.assertTrue("the isolated table must be dropped after the run", gone);
            Assert.assertTrue("the run must leave no new files in its temp directory, found " + leaked,
                    leaked.isEmpty());
            Files.delete(runTmpDir.toPath());
        } finally {
            if (originalTmpDir != null) {
                System.setProperty("java.io.tmpdir", originalTmpDir);
            }
        }
    }

    @Test(timeout = SQL_DEADLINE_MS * 3)
    public void fm14_dynamicShuffleSinkRecoversOnRealService() throws Exception {
        FAILURE_FIRED.set(false);
        SOURCE_OPENS.set(0);

        // ---- capacity precondition: refuse to start a long job against a write-throttled
        // project (HTTP 429 ODPS-0110065 "File count exceed triggered", see R-5) ----
        preflight();

        // ---- Phase 1: write NUM_ROWS ids with an injected task failure in the middle ----
        runJob("phase1", new ReplayingIdSource(NUM_ROWS, PARTITIONS, 8_000, 0L));
        Assert.assertTrue("phase 1 must have injected exactly one task failure",
                FAILURE_FIRED.get());
        Assert.assertTrue(
                "the source must have been re-opened by Flink's restart (opens=" + SOURCE_OPENS.get() + ")",
                SOURCE_OPENS.get() > PARALLELISM);

        List<String> phase1 = awaitRow(
                "select count(*) c, count(distinct id) d, min(id) mn, max(id) mx,\n"
                        + "  sum(case when score <> id then 1 else 0 end) bad,\n"
                        + "  sum(case when ds is null then 1 else 0 end) nods\n"
                        + "from " + table + ";", 6);
        long count = Long.parseLong(phase1.get(0).trim());
        Assert.assertEquals(
                "at-least-once + PK upsert must converge to exactly one row per id "
                        + "(full phase-1 readback " + phase1 + ")",
                NUM_ROWS, count);
        Assert.assertEquals("primary keys must be unique after recovery", NUM_ROWS,
                Long.parseLong(phase1.get(1).trim()));
        Assert.assertEquals("lowest id must survive", 1L, Long.parseLong(phase1.get(2).trim()));
        Assert.assertEquals("highest id must survive", (long) NUM_ROWS, Long.parseLong(phase1.get(3).trim()));
        Assert.assertEquals("every row must carry the value of its last write (idempotent upsert)",
                0L, Long.parseLong(phase1.get(4).trim()));
        Assert.assertEquals("partition column must never be lost", 0L,
                Long.parseLong(phase1.get(5).trim()));

        List<String> perPartition = awaitRow(
                "select ds, count(*) c from " + table + " group by ds order by ds;", PARTITIONS * 2);
        System.out.println("[fm14] per-partition: " + perPartition);
        Assert.assertEquals("all " + PARTITIONS + " dynamic partitions must be present",
                PARTITIONS, perPartition.size() / 2);

        // ---- Phase 2: rewrite a subset; latest value must win on the real service ----
        // phase 2 rewrites the first keys with a distinguishable score: latest must win.
        runJob("phase2", new ReplayingIdSource(REWRITE_KEYS, PARTITIONS, -1, REWRITE_SCORE_BASE));
        List<String> phase2 = awaitRow(
                "select sum(case when score <> " + REWRITE_SCORE_BASE + " + id then 1 else 0 end) stale,"
                        + " max(score) mx from " + table + " where id <= " + REWRITE_KEYS + ";", 2);
        Assert.assertEquals(
                "latest-wins must hold for the rewritten keys (stale rows readback " + phase2 + ")",
                0L, Long.parseLong(phase2.get(0).trim()));
        Assert.assertEquals("the rewritten value must be the one on disk",
                REWRITE_SCORE_BASE + REWRITE_KEYS, Long.parseLong(phase2.get(1).trim()));

        // ---- resource recycling on the writer side ----
        int threadsAfter = Thread.getAllStackTraces().size();
        System.out.printf("[fm14] threads after both jobs: %d (baseline %d, delta %d)%n",
                threadsAfter, threadsBefore, threadsAfter - threadsBefore);
        Assert.assertTrue(
                "worker threads must not grow unboundedly across two jobs with a failover "
                        + "(before " + threadsBefore + ", after " + threadsAfter + ")",
                threadsAfter - threadsBefore < 200);
    }

    /**
     * Capacity precondition: one tiny SQL insert before the real job.
     *
     * <p>Deliberately <em>not</em> time-boxed with a background thread. While the shared test
     * project is write-throttled (HTTP 429 ODPS-0110065 "File count exceed triggered", R-5) the
     * SDK retries with ~70s backoff and no cap, so an abandoned caller thread keeps the JVM alive
     * anyway and a "guard" only produces a confusing half-finished run. When the project is
     * saturated, stop here and run again later: this class is opt-in for exactly that reason.
     */
    private static void preflight() throws Exception {
        execSql("insert into " + table + " partition (ds='preflight') values (1, 1);");
        execSql("alter table " + table + " drop partition (ds='preflight');");
        System.out.println("[fm14] capacity pre-flight write accepted");
    }

    // ==========================================================================
    // job wiring
    // ==========================================================================

    private static void runJob(String phase, SourceFunction<org.apache.flink.types.Row> source)
            throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(PARALLELISM);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(1_000L);
        Configuration conf = new Configuration();
        conf.setString("io.tmp.dirs", runTmpDir.getAbsolutePath());
        conf.setString("restart-strategy", "fixed-delay");
        conf.setString("restart-strategy.fixed-delay.attempts", "5");
        conf.setString("restart-strategy.fixed-delay.delay", "2s");
        env.configure(conf, DynamicShuffleSinkRecoverySystemTest.class.getClassLoader());

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().set("table.exec.sink.upsert-materialize", "NONE");

        // Explicit RowTypeInfo with field names: a bare Row would surface as f0/f1/f2 in the view.
        org.apache.flink.api.java.typeutils.RowTypeInfo rowType =
                new org.apache.flink.api.java.typeutils.RowTypeInfo(
                        new org.apache.flink.api.common.typeinfo.TypeInformation[] {
                                org.apache.flink.api.common.typeinfo.Types.LONG,
                                org.apache.flink.api.common.typeinfo.Types.LONG,
                                org.apache.flink.api.common.typeinfo.Types.STRING},
                        new String[] {"id", "score", "ds"});
        DataStream<org.apache.flink.types.Row> data =
                env.addSource(source).returns(rowType).setParallelism(PARALLELISM);
        tEnv.createTemporaryView("src", data);

        tEnv.executeSql(
                "create table odps_sink(\n"
                + "  id bigint,\n"
                + "  score bigint,\n"
                + "  ds string,\n"
                + "  primary key(id) not enforced\n"
                + ") partitioned by (ds) with (\n"
                + "  'connector'='maxcompute',\n"
                + "  'odps.access.id'='" + odpsConf.getAccessId() + "',\n"
                + "  'odps.access.key'='" + odpsConf.getAccessKey() + "',\n"
                + "  'odps.end.point'='" + odpsConf.getEndpoint() + "',\n"
                + "  'odps.project.name'='" + odpsConf.getProject() + "',\n"
                + "  'table.name'='" + table + "',\n"
                + "  'sink.operation'='upsert',\n"
                + "  'sink.coordinator.enable'='false',\n"
                + "  'sink.dynamic-shuffle.enable'='true',\n"
                + "  'insert.writer.buffer-size'='4mb',\n"
                + "  'insert.arrow-writer.batch-size'='1024',\n"
                + "  'sink.flush.timeout'='2s'\n"
                + ")");

        long started = System.currentTimeMillis();
        org.apache.flink.table.api.TableResult result =
                tEnv.executeSql("insert into odps_sink select id, score, ds from src");
        JobClient job = result.getJobClient().orElseThrow(
                () -> new IllegalStateException("INSERT must return a job client"));
        try {
            result.await(180, TimeUnit.SECONDS);
        } finally {
            try {
                // Service throttling must not leave a live writer after a timeout/test failure.
                if (!job.getJobStatus().get(30, TimeUnit.SECONDS).isTerminalState()) {
                    job.cancel().get(60, TimeUnit.SECONDS);
                    System.out.println("[fm14] cancelled unfinished " + phase + " job=" + job.getJobID());
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
                    while (!job.getJobStatus().get(10, TimeUnit.SECONDS).isTerminalState()) {
                        if (System.nanoTime() >= deadline) {
                            throw new IllegalStateException("Cancelled job did not terminate: " + job.getJobID());
                        }
                        Thread.sleep(200L);
                    }
                }
            } finally {
                env.close();
            }
        }
        System.out.printf("[fm14] %s finished in %ds (source opens so far=%d)%n",
                phase, (System.currentTimeMillis() - started) / 1000, SOURCE_OPENS.get());
    }

    /**
     * Emits ids 1..n with {@code score = scoreBase + id}; throws exactly once at {@code failAtId}
     * to force a real task failure. Not checkpointable, so Flink's restart replays the whole
     * range, which is the at-least-once case the primary-key table has to collapse.
     */
    private static final class ReplayingIdSource
            extends org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction<
                    org.apache.flink.types.Row> {
        private static final long serialVersionUID = 1L;
        private final int numRows;
        private final int partitions;
        private final int failAtId;
        private final long scoreBase;

        ReplayingIdSource(int numRows, int partitions, int failAtId, long scoreBase) {
            this.numRows = numRows;
            this.partitions = partitions;
            this.failAtId = failAtId;
            this.scoreBase = scoreBase;
        }

        @Override
        public void run(SourceContext<org.apache.flink.types.Row> ctx) throws Exception {
            if (failAtId < 0) {
                // pre-flight pass: no failure injection, no restart accounting
            } else {
                SOURCE_OPENS.incrementAndGet();
            }
            final int subtask = getRuntimeContext().getIndexOfThisSubtask();
            final int spread = getRuntimeContext().getNumberOfParallelSubtasks();
            for (int id = 1; id <= numRows; id++) {
                if (id % spread != subtask) {
                    continue; // disjoint id ranges per instance: union == all ids, no overlap
                }
                String ds = "2024010" + (id % partitions);
                org.apache.flink.types.Row row = new org.apache.flink.types.Row(3);
                row.setField(0, (long) id);
                row.setField(1, scoreBase + id);
                row.setField(2, ds);
                ctx.collect(row);
                if (id == failAtId && FAILURE_FIRED.compareAndSet(false, true)) {
                    throw new RuntimeException(
                            "injected task failure for the real-service recovery matrix row FM-14");
                }
                if (id % 500 == 0) {
                    Thread.sleep(20L);
                }
            }
        }

        @Override
        public void cancel() {
        }
    }

    // ==========================================================================
    // SQL helpers
    // ==========================================================================

    private static List<String> awaitRow(String sql, int minTokens) throws Exception {
        long deadline = System.currentTimeMillis() + SQL_DEADLINE_MS;
        List<String> tokens = new ArrayList<>();
        while (System.currentTimeMillis() < deadline) {
            String raw = execSql(sql);
            tokens = parseResult(raw);
            if (tokens.size() >= minTokens) {
                return tokens;
            }
            System.out.printf("[fm14] readback not settled yet (%s) -> %s, retrying%n", sql, tokens);
            Thread.sleep(10_000L);
        }
        Assert.fail("SQL readback did not settle within " + SQL_DEADLINE_MS + "ms: " + sql + " -> " + tokens);
        return tokens;
    }

    /** {@code QUERY_BY_ODPS_SQL} drops the header line; do it here so multi-column rows stay aligned. */
    private static List<String> parseResult(String raw) {
        List<String> tokens = new ArrayList<>();
        String[] lines = raw.split("\\r?\\n");
        for (int i = 1; i < lines.length; i++) {
            for (String cell : lines[i].split(",")) {
                String trimmed = cell.trim();
                if (!trimmed.isEmpty()) {
                    tokens.add(trimmed);
                }
            }
        }
        return tokens;
    }

    private static String execSql(String sql) throws Exception {
        return OdpsTestUtils.exec(sql);
    }

    private static Set<String> listingOf(String dir) {
        Set<String> names = new HashSet<>();
        File[] files = new File(dir).listFiles();
        if (files != null) {
            for (File f : files) {
                names.add(f.getName());
            }
        }
        return names;
    }
}
