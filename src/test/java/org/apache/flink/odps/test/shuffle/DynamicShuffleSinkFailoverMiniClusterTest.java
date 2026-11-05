/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work by the regarding copyright ownership.
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

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.type.TypeInfoFactory;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.sink.shuffle.PartitionBucketAssignment;
import org.apache.flink.odps.sink.shuffle.PartitionBucketAssignmentRegistry;
import org.apache.flink.odps.sink.shuffle.PartitionBucketKey;
import org.apache.flink.odps.sink.shuffle.PartitionBucketKeyExtractor;
import org.apache.flink.odps.sink.shuffle.PartitionBucketPartitioner;
import org.apache.flink.odps.sink.shuffle.PartitionBucketStatisticsOperatorFactory;
import org.apache.flink.odps.sink.shuffle.RegistryAssignmentSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * FM-13: fault injection through Flink's own recovery machinery for the dynamic shuffle sink.
 *
 * <p>Where {@link DynamicShuffleFaultRecoveryMatrixTest} drives the coordinator and statistics
 * operator contract directly, this test runs the real topology on a MiniCluster with checkpointing
 * and a restart strategy, throws inside a sink subtask mid-run, and lets Flink do the failover.
 * It therefore verifies the assumptions the protocol-level matrix makes about the framework:
 * coordinator state really is restored, restarted attempts really are re-bootstrapped, and the
 * registry lifecycle really is driven by the operator's open/close.
 *
 * <p>Assertions after the injected failure:
 * <ul>
 *   <li>the job completes anyway (recovery works, the failure is not swallowed silently),</li>
 *   <li>no record is lost (at-least-once: the bounded non-checkpointed source replays, so
 *       duplicates are allowed and expected),</li>
 *   <li>every key whose routing target is invariant across all assignment versions observed
 *       during the run was written by exactly one sink subtask (single-writer, design §10.1),</li>
 *   <li>the shuffle protocol stayed active across the failover (assignment versions kept
 *       advancing after the restart),</li>
 *   <li>no registry entry survives the finished job (design §3.4 refcount / 用例 F).</li>
 * </ul>
 *
 * <p>Run: {@code mvn -Dmaven.test.skip=false test
 * -Dtest=DynamicShuffleSinkFailoverMiniClusterTest} (JDK 17: the {@code jdk17-plus} profile
 * supplies the {@code --add-opens} flags).
 */
public class DynamicShuffleSinkFailoverMiniClusterTest {

    private static final int BUCKET_NUM = 16;
    private static final int PARALLELISM = 2;
    private static final int NUM_PARTITIONS = 3;
    private static final int RECORDS_PER_PARTITION = 200;
    private static final int CONTROL_RECORDS_PER_BATCH = 4;
    /**
     * Pause between source records. The source is finite, so without pacing the whole burst
     * falls inside a single checkpoint window: the coordinator would compute exactly one
     * assignment (identical map afterwards => no further version) and the test could not see
     * any rebalancing after the failover. 5ms x 600 records spans ~15 checkpoint intervals.
     */
    private static final long SOURCE_RECORD_PAUSE_MS = 5L;

    /** (partitionSpec, bucketId) -> the sink subtask that observed each record. */
    private static final ConcurrentHashMap<String, Set<Integer>> WRITERS_BY_KEY =
            new ConcurrentHashMap<>();
    /** record id -> observed. */
    private static final Set<Long> OBSERVED_IDS = ConcurrentHashMap.newKeySet();
    /** assignment version -> the routing map that was live when the sink first saw it. */
    private static final ConcurrentHashMap<Long, Map<PartitionBucketKey, Integer>>
            VERSION_SNAPSHOTS = new ConcurrentHashMap<>();
    private static final AtomicInteger SINK_ATTEMPTS_OPENED = new AtomicInteger();
    /** versions the sink saw before / after the injected failure fired. */
    private static final Set<Long> VERSIONS_BEFORE_FAILURE = ConcurrentHashMap.newKeySet();
    private static final Set<Long> VERSIONS_AFTER_FAILURE = ConcurrentHashMap.newKeySet();
    private static final AtomicBoolean FAILURE_INJECTED = new AtomicBoolean(false);
    private static final AtomicInteger RECORDS_SEEN_BEFORE_FAILURE = new AtomicInteger();

    private String operatorUid;

    @Before
    public void resetState() {
        WRITERS_BY_KEY.clear();
        OBSERVED_IDS.clear();
        VERSION_SNAPSHOTS.clear();
        VERSIONS_BEFORE_FAILURE.clear();
        VERSIONS_AFTER_FAILURE.clear();
        SINK_ATTEMPTS_OPENED.set(0);
        FAILURE_INJECTED.set(false);
        RECORDS_SEEN_BEFORE_FAILURE.set(0);
        operatorUid = "DynamicShuffleSinkFailoverMiniClusterTest-" + System.nanoTime();
    }

    @Test
    public void fm13_dynamicShuffleRecoversFromInjectedSinkFailure() throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(PARALLELISM);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(200L);
        Configuration conf = new Configuration();
        conf.setString("restart-strategy", "fixed-delay");
        conf.setString("restart-strategy.fixed-delay.attempts", "5");
        conf.setString("restart-strategy.fixed-delay.delay", "500ms");
        env.configure(conf, DynamicShuffleSinkFailoverMiniClusterTest.class.getClassLoader());

        PartitionBucketKeyExtractor extractor = keyExtractor();

        DataStream<RowData> source =
                env.addSource(new BoundedRowSource(NUM_PARTITIONS, RECORDS_PER_PARTITION))
                        .setParallelism(1);

        DataStream<RowData> withStats = source.transform(
                "Dynamic Shuffle Statistics",
                TypeInformation.of(RowData.class),
                new PartitionBucketStatisticsOperatorFactory(
                        extractor, operatorUid, PARALLELISM, PARALLELISM))
                .setParallelism(PARALLELISM)
                .startNewChain();

        withStats.partitionCustom(
                new PartitionBucketPartitioner(new RegistryAssignmentSource(operatorUid)),
                extractor)
                .transform("FaultInjectingCollectSink",
                        TypeInformation.of(RowData.class),
                        new FaultySinkOperatorFactory(extractor, operatorUid))
                .setParallelism(PARALLELISM);

        env.execute("dynamic shuffle failover matrix");

        // ---- (1) the failure really happened, and the job still finished ----
        Assert.assertTrue("the injected sink failure never fired", FAILURE_INJECTED.get());
        Assert.assertTrue(
                "Flink did not restart the failed sink subtask (attempts="
                        + SINK_ATTEMPTS_OPENED.get() + ")",
                SINK_ATTEMPTS_OPENED.get() > PARALLELISM);

        // ---- (2) no record lost across the failover ----
        int expected = NUM_PARTITIONS * RECORDS_PER_PARTITION + 1;
        Assert.assertEquals(
                "every record must reach the sink after recovery (at-least-once, so extras are fine)",
                expected, OBSERVED_IDS.size());

        // ---- (3) the protocol stayed alive across the restart ----
        Assert.assertTrue(
                "traffic skew must make the coordinator produce more than one assignment version, "
                        + "versions seen=" + VERSION_SNAPSHOTS.keySet(),
                VERSION_SNAPSHOTS.keySet().size() >= 2);
        Assert.assertFalse(
                "the shuffle protocol must keep working after Flink restored the job; versions "
                        + "seen before the failure=" + VERSIONS_BEFORE_FAILURE
                        + ", after=" + VERSIONS_AFTER_FAILURE,
                VERSIONS_AFTER_FAILURE.isEmpty());
        long maxBefore = VERSIONS_BEFORE_FAILURE.stream().mapToLong(Long::longValue).max().orElse(0L);
        long maxAfter = VERSIONS_AFTER_FAILURE.stream().mapToLong(Long::longValue).max().orElse(0L);
        Assert.assertTrue(
                "the shuffle protocol must keep rebalancing after Flink restored the job "
                        + "(before max v" + maxBefore + ", after max v" + maxAfter + ")",
                maxAfter > maxBefore);
        for (Long version : VERSIONS_AFTER_FAILURE) {
            Assert.assertTrue(
                    "a restored coordinator must not restart the version counter from scratch "
                            + "(before max=" + maxBefore + ", after=" + VERSIONS_AFTER_FAILURE + ")",
                    version >= 1L);
        }

        // ---- (4) single-writer for keys whose target never moved ----
        // Assignment maps are window-scoped (the coordinator clears aggregatedStats after every
        // checkpoint), so a key that had no traffic in one window is simply absent from that
        // version's map and falls back to the static hash. A key is therefore only expected to
        // have a single writer when its assigned target and its fallback target agree in every
        // version that contains it; those are the keys checked below, and any other key is
        // allowed to move.
        int stableKeys = 0;
        int reassignedKeys = 0;
        List<String> splitStableKeys = new ArrayList<>();
        for (Map.Entry<String, Set<Integer>> entry : WRITERS_BY_KEY.entrySet()) {
            PartitionBucketKey key = parseKey(entry.getKey());
            Set<Integer> expectedWriters = new HashSet<>();
            expectedWriters.add(Math.floorMod(key.hashCode(), PARALLELISM));
            boolean presentInSomeVersion = false;
            for (Map<PartitionBucketKey, Integer> map : VERSION_SNAPSHOTS.values()) {
                Integer target = map.get(key);
                if (target == null) {
                    continue;
                }
                presentInSomeVersion = true;
                expectedWriters.add(target);
                if (expectedWriters.size() > 1) {
                    break;
                }
            }
            if (!presentInSomeVersion || expectedWriters.size() > 1) {
                reassignedKeys++;
                continue;
            }
            stableKeys++;
            if (!entry.getValue().equals(expectedWriters)) {
                splitStableKeys.add(entry.getKey() + " writers=" + entry.getValue()
                        + " expected=" + expectedWriters);
            }
        }
        Assert.assertFalse(
                "no key ended up with an invariant writer; check the topology actually routed",
                stableKeys == 0);
        Assert.assertTrue(
                "a (partitionSpec, bucketId) group with an invariant writer must never be written "
                        + "by another sink subtask (design §10.1), offenders: " + splitStableKeys,
                splitStableKeys.isEmpty());

        PartitionBucketKey control = extractor.getKey(controlRow());
        Assert.assertTrue("the control key must appear in a dynamic assignment",
                VERSION_SNAPSHOTS.values().stream().anyMatch(map -> map.containsKey(control)));
        for (Map<PartitionBucketKey, Integer> map : VERSION_SNAPSHOTS.values()) {
            if (map.containsKey(control)) {
                Assert.assertEquals("the dominant control key must stay on its fallback writer",
                        Integer.valueOf(0), map.get(control));
            }
        }
        Assert.assertEquals("the control key must have exactly one writer across recovery",
                Collections.singleton(0), WRITERS_BY_KEY.get(keyString(control)));

        // ---- (5) registry lifecycle: nothing may survive the job ----
        Assert.assertTrue(
                "the operator registration must be released when the job finishes (leak: "
                        + currentAssignment(operatorUid).getVersion() + ")",
                currentAssignment(operatorUid).isEmpty());

        System.out.printf(
                "%n[fm13] records=%d/%d sinkAttemptOpens=%d versionsBefore=%s versionsAfter=%s "
                        + "stableKeys=%d movingKeys=%d splitStableKeys=%d%n",
                OBSERVED_IDS.size(), expected, SINK_ATTEMPTS_OPENED.get(),
                VERSIONS_BEFORE_FAILURE, VERSIONS_AFTER_FAILURE,
                stableKeys, reassignedKeys, splitStableKeys.size());
    }

    // ==========================================================================
    // topology pieces
    // ==========================================================================

    /** Bounded, non-checkpointed source: a failover replays it, which is the at-least-once case. */
    private static final class BoundedRowSource implements SourceFunction<RowData> {
        private static final long serialVersionUID = 1L;
        private final int numPartitions;
        private final int recordsPerPartition;
        private volatile boolean running = true;

        BoundedRowSource(int numPartitions, int recordsPerPartition) {
            this.numPartitions = numPartitions;
            this.recordsPerPartition = recordsPerPartition;
        }

        @Override
        public void run(SourceContext<RowData> ctx) throws Exception {
            for (long id = 0; id < (long) numPartitions * recordsPerPartition && running; id++) {
                // Phase 1 spreads evenly over the partitions, phase 2 collapses almost all
                // traffic onto one of them: the aggregate per (partition, bucket) changes, so
                // the coordinator has to produce a new assignment version mid-run. Without this
                // skew the map is identical at every checkpoint and the version never advances
                // (measured: only v1 was ever computed).
                int partitionIndex;
                if (id < (long) numPartitions * recordsPerPartition / 2) {
                    partitionIndex = (int) (id % numPartitions);
                } else {
                    partitionIndex = (id % 10 == 0) ? (int) (id % numPartitions) : 0;
                }
                GenericRowData row = new GenericRowData(3);
                row.setField(0, id);
                row.setField(1, StringData.fromString("name_" + id));
                row.setField(2, StringData.fromString("ds=2024010" + partitionIndex));
                synchronized (ctx.getCheckpointLock()) {
                    // Each complete checkpoint window contains four control records per
                    // ordinary record. The control is strictly the heaviest key, so the
                    // greedy assignment always places it on subtask 0. Holding the source's
                    // checkpoint lock across the whole batch prevents a barrier from splitting
                    // that ratio. Its static hash also selects 0, including empty windows.
                    // The old fixture merely hoped one of the 48 ordinary keys would never
                    // move; window timing could move all of them and fail a correct job.
                    for (int i = 0; i < CONTROL_RECORDS_PER_BATCH; i++) {
                        ctx.collect(controlRow());
                    }
                    ctx.collect(row);
                }
                Thread.sleep(SOURCE_RECORD_PAUSE_MS);
            }
            // Keep the job alive long enough for several checkpoints and the post-restart ones.
            Thread.sleep(3_000L);
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static final class FaultySinkOperatorFactory
            extends org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory<RowData>
            implements OneInputStreamOperatorFactory<RowData, RowData> {

        private final PartitionBucketKeyExtractor extractor;
        private final String operatorUid;

        FaultySinkOperatorFactory(PartitionBucketKeyExtractor extractor, String operatorUid) {
            this.extractor = extractor;
            this.operatorUid = operatorUid;
        }

        @Override
        public <O extends StreamOperator<RowData>> O createStreamOperator(
                StreamOperatorParameters<RowData> parameters) {
            FaultySinkOperator operator = new FaultySinkOperator(extractor, operatorUid);
            operator.setup(
                    parameters.getContainingTask(),
                    parameters.getStreamConfig(),
                    parameters.getOutput());
            @SuppressWarnings("unchecked")
            O result = (O) operator;
            return result;
        }

        @Override
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return FaultySinkOperator.class;
        }
    }

    /** Records what each sink subtask observed, then fails once to trigger a real failover. */
    private static final class FaultySinkOperator extends AbstractStreamOperator<RowData>
            implements OneInputStreamOperator<RowData, RowData> {

        private static final long serialVersionUID = 1L;
        private static final int FAIL_AFTER_RECORDS = 150;

        private final PartitionBucketKeyExtractor extractor;
        private final String operatorUid;
        private int subtaskIndex;

        FaultySinkOperator(PartitionBucketKeyExtractor extractor, String operatorUid) {
            this.extractor = extractor;
            this.operatorUid = operatorUid;
        }

        @Override
        public void open() throws Exception {
            super.open();
            subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
            SINK_ATTEMPTS_OPENED.incrementAndGet();
        }

        @Override
        public void processElement(StreamRecord<RowData> element) throws Exception {
            RowData row = element.getValue();
            PartitionBucketKey key = extractor.getKey(row);
            long id = row.getLong(0);

            PartitionBucketAssignment assignment = currentAssignment(operatorUid);
            if (!assignment.isEmpty()) {
                VERSION_SNAPSHOTS.putIfAbsent(
                        assignment.getVersion(), new HashMap<>(assignment.getAssignment()));
                if (FAILURE_INJECTED.get()) {
                    VERSIONS_AFTER_FAILURE.add(assignment.getVersion());
                } else {
                    VERSIONS_BEFORE_FAILURE.add(assignment.getVersion());
                }
            }
            WRITERS_BY_KEY
                    .computeIfAbsent(keyString(key), k -> ConcurrentHashMap.newKeySet())
                    .add(subtaskIndex);
            OBSERVED_IDS.add(id);

            if (id >= 0 && RECORDS_SEEN_BEFORE_FAILURE.incrementAndGet() == FAIL_AFTER_RECORDS
                    && FAILURE_INJECTED.compareAndSet(false, true)) {
                throw new RuntimeException(
                        "injected sink failure for the dynamic shuffle fault-recovery matrix");
            }
        }
    }

    // ==========================================================================
    // helpers
    // ==========================================================================

    /** Reads the registry exactly the way the production partitioner does, for this job's uid. */
    private static PartitionBucketAssignment currentAssignment(String operatorUid) {
        return PartitionBucketAssignmentRegistry.getInstance().getAssignment(operatorUid);
    }

    private static PartitionBucketKeyExtractor keyExtractor() {
        RowType flinkSchema = new RowType(Arrays.asList(
                new RowType.RowField("id", new BigIntType()),
                new RowType.RowField("name", new VarCharType()),
                new RowType.RowField("ds", new VarCharType())));
        List<Column> odpsColumns = Arrays.asList(
                new Column("id", TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.BIGINT)),
                new Column("name", TypeInfoFactory.getVarcharTypeInfo(255)),
                new Column("ds", TypeInfoFactory.getVarcharTypeInfo(255)));
        return new PartitionBucketKeyExtractor(
                odpsColumns,
                Collections.singletonList("id"),
                Collections.singletonList("ds"),
                BUCKET_NUM,
                Collections.emptyMap(),
                null,
                flinkSchema);
    }

    private static GenericRowData controlRow() {
        PartitionBucketKeyExtractor extractor = keyExtractor();
        for (long id = -1; id >= -BUCKET_NUM * 2; id--) {
            GenericRowData row = GenericRowData.of(id, StringData.fromString("control"),
                    StringData.fromString("ds=control"));
            if (Math.floorMod(extractor.getKey(row).hashCode(), PARALLELISM) == 0) {
                return row;
            }
        }
        throw new AssertionError("cannot construct a control key with fallback writer 0");
    }

    private static String keyString(PartitionBucketKey key) {
        return key.getPartitionSpec() + "#" + key.getBucketId();
    }

    private static PartitionBucketKey parseKey(String encoded) {
        int split = encoded.lastIndexOf('#');
        return new PartitionBucketKey(
                encoded.substring(0, split), Integer.parseInt(encoded.substring(split + 1)));
    }
}
