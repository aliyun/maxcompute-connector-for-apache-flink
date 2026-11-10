/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.odps.test;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.task.SQLTask;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.odps.test.util.MaxComputeTestEnvironment;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.*;

/** Real Storage V2 SQL upsert, independently checked by MaxCompute SQL after checkpoints. */
@RunWith(Parameterized.class)
public class StorageV2PkChangelogITCase {
    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> cases() {
        String selected = System.getProperty("maxcompute.e2e.pk-mode", "all");
        List<Object[]> cases = new ArrayList<>();
        for (String mode : Arrays.asList("unpartitioned", "static", "dynamic"))
            if (selected.equals("all") || selected.equals(mode)) cases.add(new Object[] {mode});
        if (cases.isEmpty()) throw new IllegalArgumentException("Unknown PK test mode: " + selected);
        return cases;
    }

    private final String mode;

    public StorageV2PkChangelogITCase(String mode) {
        this.mode = mode;
    }

    @Test(timeout = 900000)
    public void updatesDeletesReplayAndReinsertSurviveCheckpointFlush() throws Exception {
        OdpsConf identity = MaxComputeTestEnvironment.getOdpsConf();
        Odps odps = OdpsUtils.getOdps(identity);
        odps.setCurrentSchema("default");
        odps.getRestClient().setConnectTimeout(10);
        odps.getRestClient().setReadTimeout(30);
        boolean partitioned = !mode.equals("unpartitioned");
        boolean dynamic = mode.equals("dynamic");
        String table = "flink_v2_pk_" + UUID.randomUUID().toString().replace("-", "");
        String qualified = identity.getProject() + ".default." + table;
        Path directory = Files.createTempDirectory("flink-v2-pk-");
        StreamExecutionEnvironment env = null;
        JobClient job = null;
        CompletableFuture<JobExecutionResult> completion = null;
        boolean attempted = false;
        try {
            assertFalse(odps.tables().exists(identity.getProject(), "default", table));
            attempted = true;
            sql(odps, "CREATE TABLE " + qualified
                    + "(id BIGINT NOT NULL PRIMARY KEY,score BIGINT,note STRING)"
                    + (partitioned ? " PARTITIONED BY(ds STRING)" : "")
                    + " TBLPROPERTIES('transactional'='true','write.bucket.num'='4') LIFECYCLE 1;");
            // Changelog correctness uses existing partitions. Automatic partition publication
            // has a separate visibility boundary and must not gate the first update/delete.
            if (partitioned) {
                sql(odps, "ALTER TABLE " + qualified + " ADD IF NOT EXISTS PARTITION(ds='a');");
                if (dynamic)
                    sql(odps, "ALTER TABLE " + qualified + " ADD IF NOT EXISTS PARTITION(ds='b');");
            }
            Configuration runtime = new Configuration();
            runtime.setString("rest.bind-port", "0");
            runtime.setString("io.tmp.dirs", directory.resolve("runtime").toString());
            runtime.setString("state.checkpoints.dir", directory.resolve("checkpoints").toUri().toString());
            runtime.setString("restart-strategy", "none");
            env = StreamExecutionEnvironment.createLocalEnvironment(2, runtime);
            env.enableCheckpointing(1000);
            StreamTableEnvironment tables = StreamTableEnvironment.create(env);
            List<List<Row>> phases = phases(dynamic);
            RowTypeInfo type = new RowTypeInfo(
                    new org.apache.flink.api.common.typeinfo.TypeInformation<?>[] {
                            Types.LONG, Types.LONG, Types.STRING, Types.STRING},
                    new String[] {"id", "score", "note", "ds"});
            Schema.Builder sourceSchema = Schema.newBuilder()
                    .column("id", DataTypes.BIGINT().notNull())
                    .column("score", DataTypes.BIGINT())
                    .column("note", DataTypes.STRING())
                    .column("ds", DataTypes.STRING().notNull());
            // Projecting ds out of a composite source key would lose the upsert key.
            // Flink would then need full retract values, rather than key-only deletes.
            sourceSchema.primaryKey(dynamic ? new String[] {"id", "ds"} : new String[] {"id"});
            Table changes = tables.fromChangelogStream(
                    env.addSource(new PhasedSource(phases, directory.toString()))
                            .returns(type).setParallelism(1), sourceSchema.build());
            tables.createTemporaryView("changes", changes);
            Schema.Builder sinkSchema = Schema.newBuilder()
                    .column("id", DataTypes.BIGINT().notNull())
                    .column("score", DataTypes.BIGINT())
                    .column("note", DataTypes.STRING());
            if (partitioned) sinkSchema.column("ds", DataTypes.STRING().notNull());
            sinkSchema.primaryKey(partitioned ? new String[] {"id", "ds"} : new String[] {"id"});
            TableDescriptor.Builder sink = TableDescriptor.forConnector("maxcompute")
                    .schema(sinkSchema.build())
                    .option("odps.project.name", identity.getProject())
                    .option("odps.end.point", identity.getEndpoint())
                    .option("odps.access.id", identity.getAccessId())
                    .option("odps.access.key", identity.getAccessKey())
                    .option("odps.namespace.schema", "true")
                    .option("table.name", identity.getProject() + ".default." + table)
                    .option("sink.operation", "upsert")
                    .option("sink.coordinator.enable", "false")
                    .option("insert.arrow-writer.batch-size", "2")
                    .option("insert.writer.buffer-size", "1mb")
                    .option("sink.flush.timeout", "1s");
            String token = identity.getProperty("odps.access.security.token");
            if (token != null) sink.option("odps.access.security.token", token);
            if (partitioned) sink.partitionedBy("ds");
            tables.createTemporaryTable("pk_sink", sink.build());
            String insert = "INSERT INTO pk_sink" + (mode.equals("static") ? " PARTITION(ds='a')" : "")
                    + " SELECT id,score,note"
                    + (dynamic ? ",ds" : "") + " FROM changes";
            System.out.printf("[v2-pk] START mode=%s table=%s%n", mode, qualified);
            TableResult result = tables.executeSql(insert);
            job = result.getJobClient().orElseThrow(() -> new IllegalStateException("Missing job"));
            completion = job.getJobExecutionResult();
            List<List<String>> expected = expected(dynamic, partitioned);
            for (int phase = 0; phase < phases.size(); phase++) {
                awaitSignal(directory.resolve("committed-" + phase), completion);
                assertRows(odps, qualified, partitioned, expected.get(phase));
                System.out.printf("[v2-pk] PASS mode=%s phase=%d input=%s rows=%s%n",
                        mode, phase, phases.get(phase), expected.get(phase));
                Files.createFile(directory.resolve("release-" + phase));
            }
            result.await(60, TimeUnit.SECONDS);
        } finally {
            try {
                // The local executor can shut its MiniCluster down immediately on completion.
                // Its retained result future remains valid after getJobStatus RPCs stop working.
                if (job != null && !completion.isDone()) {
                    job.cancel().get(30, TimeUnit.SECONDS);
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
                    while (!completion.isDone()) {
                        if (System.nanoTime() > deadline) throw new IllegalStateException("Job cancellation timed out");
                        Thread.sleep(200);
                    }
                }
            } finally {
                try {
                    if (env != null) env.close();
                } finally {
                    try {
                        if (attempted) {
                            sql(odps, "DROP TABLE IF EXISTS " + qualified + ";");
                            assertFalse("Table cleanup", odps.tables().exists(identity.getProject(), "default", table));
                            System.out.printf("[v2-pk] CLEANUP table=%s absent=true%n", qualified);
                        }
                    } finally {
                        try (Stream<Path> paths = Files.walk(directory)) {
                            for (Path path : paths.sorted(Collections.reverseOrder()).collect(Collectors.toList()))
                                Files.delete(path);
                        }
                        assertFalse("Checkpoint and local runtime cleanup", Files.exists(directory));
                    }
                }
            }
        }
    }

    private static Row row(RowKind kind, long id, Long score, String note, String ds) {
        return Row.ofKind(kind, id, score, note, ds);
    }

    private static List<List<Row>> phases(boolean dynamic) {
        List<Row> initial = new ArrayList<>(Arrays.asList(
                row(RowKind.INSERT, 1, 10L, "keep", "a"),
                row(RowKind.INSERT, 2, 20L, "remove", "a"),
                row(RowKind.INSERT, 3, 30L, "nullable", "a"),
                row(RowKind.INSERT, 4, 40L, "old-key", "a")));
        if (dynamic) initial.add(row(RowKind.INSERT, 1, 100L, "other-partition", "b"));
        List<Row> mutations = Arrays.asList(
                row(RowKind.UPDATE_BEFORE, 1, 10L, "keep", "a"),
                row(RowKind.UPDATE_AFTER, 1, 11L, "updated", "a"),
                row(RowKind.UPDATE_AFTER, 3, null, null, "a"),
                row(RowKind.DELETE, 2, null, null, "a"),
                row(RowKind.DELETE, 999, null, null, "a"),
                row(RowKind.DELETE, 4, null, null, "a"),
                row(RowKind.INSERT, 5, 50L, "new-key", "a"));
        List<Row> replay = new ArrayList<>(mutations);
        replay.add(row(RowKind.UPDATE_AFTER, 1, 12L, "intermediate", "a"));
        replay.add(row(RowKind.UPDATE_AFTER, 1, 13L, "latest", "a"));
        replay.add(row(RowKind.INSERT, 2, 200L, "reinserted", "a"));
        replay.add(row(RowKind.DELETE, 3, null, null, "a"));
        List<Row> last = Arrays.asList(
                row(RowKind.DELETE, 1, null, null, "a"),
                row(RowKind.DELETE, 1, null, null, "a"),
                row(RowKind.UPDATE_AFTER, 2, null, "中文🙂", "a"),
                row(RowKind.INSERT, 6, 60L, "added", "a"));
        return Arrays.asList(initial, mutations, replay, last);
    }

    private static List<List<String>> expected(boolean dynamic, boolean partitioned) {
        List<List<String>> result = new ArrayList<>();
        for (List<String> values : Arrays.asList(
                Arrays.asList("1|10|keep", "2|20|remove", "3|30|nullable", "4|40|old-key"),
                Arrays.asList("1|11|updated", "3|#NULL|#NULL", "5|50|new-key"),
                Arrays.asList("1|13|latest", "2|200|reinserted", "5|50|new-key"),
                Arrays.asList("2|#NULL|中文🙂", "5|50|new-key", "6|60|added"))) {
            List<String> rows = values.stream().map(v -> v + (partitioned ? "|a" : "")).collect(Collectors.toList());
            if (dynamic) rows.add("1|100|other-partition|b");
            Collections.sort(rows);
            result.add(rows);
        }
        return result;
    }

    private static void assertRows(Odps odps, String table, boolean partitioned, List<String> expected) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(4);
        List<String> rows;
        do {
            // SQLTask's CSV result decoder can expose SQL NULL as the literal \N.
            // Normalize in the independent SQL query, before SDK result decoding.
            Instance query = sql(odps, "SELECT id,coalesce(cast(score AS STRING),'#NULL'),"
                    + "coalesce(note,'#NULL')" + (partitioned ? ",ds" : "") + " FROM " + table + ";");
            rows = new ArrayList<>();
            for (Record record : SQLTask.getResult(query)) {
                List<String> cells = new ArrayList<>();
                for (int i = 0; i < (partitioned ? 4 : 3); i++)
                    cells.add(record.get(i) == null ? "#NULL" : record.get(i).toString());
                rows.add(String.join("|", cells));
            }
            Collections.sort(rows);
            if (rows.equals(expected)) return;
            System.out.printf("[v2-pk] visibility retry expected=%s actual=%s%n", expected, rows);
            Thread.sleep(2000);
        } while (System.nanoTime() < deadline);
        assertEquals("Exact SQL rows, including duplicates and deleted keys", expected, rows);
    }

    private static Instance sql(Odps odps, String statement) throws Exception {
        Map<String, String> hints = new HashMap<>();
        hints.put("odps.sql.type.system.odps2", "true");
        hints.put("odps.sql.allow.fullscan", "true");
        hints.put("odps.namespace.schema", "true");
        hints.put("odps.sql.allow.namespace.schema", "true");
        Instance instance = SQLTask.run(odps, odps.getDefaultProject(), statement, hints, null);
        long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(3);
        System.out.printf("[v2-pk] SQL_SUBMIT instance=%s operation=%s%n", instance.getId(), statement.split(" ")[0]);
        while (!instance.isTerminated()) {
            if (System.nanoTime() > deadline) {
                instance.stop();
                throw new IllegalStateException("SQL timeout: " + instance.getId());
            }
            Thread.sleep(1000);
        }
        instance.waitForSuccess();
        System.out.printf("[v2-pk] SQL instance=%s operation=%s%n", instance.getId(), statement.split(" ")[0]);
        return instance;
    }

    private static void awaitSignal(Path signal, CompletableFuture<JobExecutionResult> completion) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(3);
        while (!Files.exists(signal)) {
            assertFalse("Job terminated before phase checkpoint: " + signal.getFileName(),
                    completion.isDone());
            if (System.nanoTime() > deadline) throw new IllegalStateException("Checkpoint timeout: " + signal.getFileName());
            Thread.sleep(100);
        }
    }

    /** Holds each phase open until its checkpoint and independent SQL assertions complete. */
    private static final class PhasedSource implements SourceFunction<Row>, CheckpointedFunction, CheckpointListener {
        private final List<List<Row>> phases;
        private final String directory;
        private volatile boolean running = true;
        private volatile int phase = -1;
        private final Map<Long, Integer> snapshots = new TreeMap<>();

        private PhasedSource(List<List<Row>> phases, String directory) {
            this.phases = phases;
            this.directory = directory;
        }

        @Override
        public void run(SourceContext<Row> context) throws Exception {
            for (int next = 0; running && next < phases.size(); next++) {
                synchronized (context.getCheckpointLock()) {
                    for (Row row : phases.get(next)) context.collect(row);
                    phase = next;
                }
                Path release = Paths.get(directory, "release-" + next);
                while (running && !Files.exists(release)) Thread.sleep(100);
            }
        }

        @Override
        public void cancel() { running = false; }

        @Override
        public void initializeState(FunctionInitializationContext context) {
            if (context.isRestored()) throw new IllegalStateException("This test does not validate recovery");
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) { snapshots.put(context.getCheckpointId(), phase); }

        @Override
        public void notifyCheckpointComplete(long id) throws Exception {
            Integer completed = snapshots.remove(id);
            if (completed != null && completed >= 0) {
                Path signal = Paths.get(directory, "committed-" + completed);
                if (!Files.exists(signal)) {
                    Files.createFile(signal);
                    System.out.printf("[v2-pk] CHECKPOINT id=%d phase=%d%n", id, completed);
                }
            }
            snapshots.keySet().removeIf(checkpoint -> checkpoint < id);
        }
    }
}
