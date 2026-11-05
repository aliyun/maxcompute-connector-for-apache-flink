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

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.type.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.odps.sink.shuffle.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * End-to-end smoke test for the Dynamic Shuffle topology on a local Flink execution environment.
 * <p>
 * Topology:
 *   Source (generate RowData)
 *     → StatisticsOperator (tracks per-key traffic, reports at checkpoint)
 *     → partitionCustom(PartitionBucketPartitioner)
 *     → CollectSink (records which subtask received which key)
 * <p>
 * Verifies:
 *   1. The topology runs without errors
 *   2. All records are delivered to the sink
 *   3. The Coordinator computes assignment correctly (3 partitions × 16 buckets = 48 groups)
 *   4. After the assignment stabilizes, the partitioner routes consistently
 *   5. Partitioner determinism: given the same assignment, partition() always returns the same subtask
 */
public class DynamicShuffleSmokeTest {

    private static final int PARALLELISM = 4;
    private static final int BUCKET_NUM = 16;
    private static final int NUM_PARTITIONS = 3;
    private static final int RECORDS_PER_PARTITION = 500;

    /**
     * Collected results per subtask.
     * Key: subtask index, Value: list of (partitionSpec, bucketId, rowId)
     */
    private static final ConcurrentHashMap<Integer, List<Tuple3<String, Integer, Integer>>>
            COLLECTED = new ConcurrentHashMap<>();

    @Before
    public void setup() {
        COLLECTED.clear();
    }

    @After
    public void teardown() {
        COLLECTED.clear();
    }

    /**
     * Test 1: Full topology e2e — verifies the topology runs, data flows through,
     * and the coordinator computes the correct assignment.
     */
    @Test
    public void testDynamicShuffleTopology() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(PARALLELISM);
        env.enableCheckpointing(500);

        RowType flinkSchema = new RowType(Arrays.asList(
                new RowType.RowField("id", new BigIntType()),
                new RowType.RowField("name", new VarCharType()),
                new RowType.RowField("ds", new VarCharType())
        ));

        List<Column> odpsColumns = Arrays.asList(
                new Column("id", TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.BIGINT)),
                new Column("name", TypeInfoFactory.getVarcharTypeInfo(255)),
                new Column("ds", TypeInfoFactory.getVarcharTypeInfo(255))
        );

        PartitionBucketKeyExtractor keyExtractor = new PartitionBucketKeyExtractor(
                odpsColumns,
                Collections.singletonList("id"),
                Collections.singletonList("ds"),
                BUCKET_NUM,
                Collections.emptyMap(),
                null,
                flinkSchema
        );

        String operatorUid = "e2e-test-shuffle";
        PartitionBucketAssignmentRegistry.getInstance().register(operatorUid);

        // Source
        DataStream<RowData> source = env.addSource(
                new TestSourceFunction(NUM_PARTITIONS, RECORDS_PER_PARTITION))
                .setParallelism(1);

        // Statistics operator
        PartitionBucketStatisticsOperatorFactory statsFactory =
                new PartitionBucketStatisticsOperatorFactory(keyExtractor, operatorUid, PARALLELISM, PARALLELISM);

        DataStream<RowData> withStats = source
                .transform("Dynamic Shuffle Statistics",
                        TypeInformation.of(RowData.class),
                        statsFactory)
                .setParallelism(PARALLELISM);

        // Partition
        DataStream<RowData> shuffled = withStats
                .partitionCustom(new PartitionBucketPartitioner(new RegistryAssignmentSource(operatorUid)), keyExtractor);

        // Collect sink
        shuffled.transform("CollectSink",
                TypeInformation.of(RowData.class),
                new CollectSinkOperatorFactory())
                .setParallelism(PARALLELISM);

        // Execute
        env.execute("Dynamic Shuffle Smoke Test");

        // =====================================================================
        // Verify
        // =====================================================================

        int totalCollected = COLLECTED.values().stream().mapToInt(List::size).sum();
        int expectedTotal = NUM_PARTITIONS * RECORDS_PER_PARTITION;

        System.out.printf("%n=== E2E Results ===%n");
        System.out.printf("Collected %d / %d records%n", totalCollected, expectedTotal);

        // 1. All records collected
        assertEquals("All records should be collected", expectedTotal, totalCollected);

        // 2. Check assignment was computed by the coordinator
        PartitionBucketAssignment assignment =
                PartitionBucketAssignmentRegistry.getInstance().getAssignment(operatorUid);
        assertNotNull("Assignment should be computed", assignment);
        assertFalse("Assignment should not be empty", assignment.isEmpty());

        int expectedGroups = NUM_PARTITIONS * BUCKET_NUM; // 3 * 16 = 48
        System.out.printf("Assignment version=%d, groups=%d (expected %d)%n",
                assignment.getVersion(), assignment.getAssignment().size(), expectedGroups);
        assertEquals("Assignment should have correct number of groups",
                expectedGroups, assignment.getAssignment().size());

        // 3. Print subtask distribution
        System.out.println("=== Subtask Distribution ===");
        for (Map.Entry<Integer, List<Tuple3<String, Integer, Integer>>> entry :
                COLLECTED.entrySet()) {
            System.out.printf("  Subtask %d: %d records%n", entry.getKey(), entry.getValue().size());
        }

        // Cleanup
        PartitionBucketAssignmentRegistry.getInstance().removeAssignment(operatorUid);
    }

    /**
     * Test 2: Partitioner determinism — given a fixed assignment,
     * the partitioner always routes the same key to the same subtask.
     * This tests the partitioner in isolation (no topology needed).
     */
    @Test
    public void testPartitionerDeterminismWithAssignment() {
        String operatorUid = "determinism-test";
        PartitionBucketAssignmentRegistry.getInstance().register(operatorUid);

        try {
            // Create an assignment with known routing
            Map<PartitionBucketKey, Integer> assignmentMap = new HashMap<>();
            for (int p = 0; p < NUM_PARTITIONS; p++) {
                String partitionSpec = "ds=2024010" + p;
                for (int b = 0; b < BUCKET_NUM; b++) {
                    PartitionBucketKey key = new PartitionBucketKey(partitionSpec, b);
                    // Deterministic: distribute using round-robin
                    int subtask = (p * BUCKET_NUM + b) % PARALLELISM;
                    assignmentMap.put(key, subtask);
                }
            }
            PartitionBucketAssignment assignment = new PartitionBucketAssignment(assignmentMap, 1);
            PartitionBucketAssignmentRegistry.getInstance().updateAssignment(operatorUid, assignment);

            // Test the partitioner
            PartitionBucketPartitioner partitioner =
                    new PartitionBucketPartitioner(new RegistryAssignmentSource(operatorUid));

            // For each key, verify partition() returns the same value 100 times
            for (Map.Entry<PartitionBucketKey, Integer> entry : assignmentMap.entrySet()) {
                PartitionBucketKey key = entry.getKey();
                int expectedSubtask = entry.getValue();
                for (int i = 0; i < 100; i++) {
                    int actual = partitioner.partition(key, PARALLELISM);
                    assertEquals("Key " + key + " should always route to " + expectedSubtask,
                            expectedSubtask, actual);
                }
            }

            System.out.printf("Partitioner determinism: all %d keys verified × 100 calls%n",
                    assignmentMap.size());
        } finally {
            PartitionBucketAssignmentRegistry.getInstance().removeAssignment(operatorUid);
        }
    }

    /**
     * Test 3: Verify that the final assignment from the e2e topology produces
     * consistent routing — i.e., if we feed the assignment to the partitioner,
     * every key has a single deterministic subtask.
     */
    @Test
    public void testFinalAssignmentConsistency() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(PARALLELISM);
        env.enableCheckpointing(500);

        RowType flinkSchema = new RowType(Arrays.asList(
                new RowType.RowField("id", new BigIntType()),
                new RowType.RowField("name", new VarCharType()),
                new RowType.RowField("ds", new VarCharType())
        ));

        List<Column> odpsColumns = Arrays.asList(
                new Column("id", TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.BIGINT)),
                new Column("name", TypeInfoFactory.getVarcharTypeInfo(255)),
                new Column("ds", TypeInfoFactory.getVarcharTypeInfo(255))
        );

        PartitionBucketKeyExtractor keyExtractor = new PartitionBucketKeyExtractor(
                odpsColumns,
                Collections.singletonList("id"),
                Collections.singletonList("ds"),
                BUCKET_NUM,
                Collections.emptyMap(),
                null,
                flinkSchema
        );

        String operatorUid = "consistency-test-shuffle";
        PartitionBucketAssignmentRegistry.getInstance().register(operatorUid);

        DataStream<RowData> source = env.addSource(
                new TestSourceFunction(NUM_PARTITIONS, RECORDS_PER_PARTITION))
                .setParallelism(1);

        PartitionBucketStatisticsOperatorFactory statsFactory =
                new PartitionBucketStatisticsOperatorFactory(keyExtractor, operatorUid, PARALLELISM, PARALLELISM);

        DataStream<RowData> withStats = source
                .transform("Dynamic Shuffle Statistics",
                        TypeInformation.of(RowData.class),
                        statsFactory)
                .setParallelism(PARALLELISM);

        DataStream<RowData> shuffled = withStats
                .partitionCustom(new PartitionBucketPartitioner(new RegistryAssignmentSource(operatorUid)), keyExtractor);

        shuffled.transform("CollectSink",
                TypeInformation.of(RowData.class),
                new CollectSinkOperatorFactory())
                .setParallelism(PARALLELISM);

        env.execute("Assignment Consistency Test");

        // Now verify: given the final assignment, partitioner routing is consistent
        PartitionBucketAssignment assignment =
                PartitionBucketAssignmentRegistry.getInstance().getAssignment(operatorUid);
        assertNotNull("Assignment should exist", assignment);
        assertFalse("Assignment should not be empty", assignment.isEmpty());

        PartitionBucketPartitioner partitioner =
                new PartitionBucketPartitioner(new RegistryAssignmentSource(operatorUid));

        // Every key in the assignment should route consistently
        int consistentKeys = 0;
        for (Map.Entry<PartitionBucketKey, Integer> entry : assignment.getAssignment().entrySet()) {
            PartitionBucketKey key = entry.getKey();
            int assignedSubtask = entry.getValue();
            int partitionResult = partitioner.partition(key, PARALLELISM);
            assertEquals("Key " + key + " assigned to " + assignedSubtask +
                    " but partitioner returned " + partitionResult,
                    assignedSubtask, partitionResult);
            consistentKeys++;
        }

        System.out.printf("Final assignment consistency: %d keys all consistent%n", consistentKeys);

        // Verify coverage: all (partition, bucket) combos are in the assignment
        assertEquals("All (partition, bucket) combos should be assigned",
                NUM_PARTITIONS * BUCKET_NUM, assignment.getAssignment().size());

        // Verify balance: no subtask should have 0 groups
        Map<Integer, Integer> subtaskGroupCount = new HashMap<>();
        for (int subtask : assignment.getAssignment().values()) {
            subtaskGroupCount.merge(subtask, 1, Integer::sum);
        }
        for (int i = 0; i < PARALLELISM; i++) {
            assertTrue("Subtask " + i + " should have at least 1 group",
                    subtaskGroupCount.getOrDefault(i, 0) > 0);
        }

        System.out.println("Balance check: " + subtaskGroupCount);

        PartitionBucketAssignmentRegistry.getInstance().removeAssignment(operatorUid);
    }

    /**
     * Test 4: Fallback behavior — when no assignment exists, the partitioner uses
     * {@code floorMod(key.hashCode(), numPartitions)}, which mixes partitionSpec and
     * bucketId so partitions are spread across subtasks instead of collapsing onto
     * {@code bucketId % numPartitions}.
     */
    @Test
    public void testPartitionerFallback() {
        String operatorUid = "fallback-test";
        // Don't register — no assignment will exist

        PartitionBucketPartitioner partitioner =
                new PartitionBucketPartitioner(new RegistryAssignmentSource(operatorUid));

        for (int b = 0; b < BUCKET_NUM; b++) {
            PartitionBucketKey key = new PartitionBucketKey("ds=test", b);
            int expected = Math.floorMod(key.hashCode(), PARALLELISM);
            int actual = partitioner.partition(key, PARALLELISM);
            assertEquals("Fallback: key " + key + " should route to " + expected,
                    expected, actual);
        }

        System.out.println("Fallback routing verified for " + BUCKET_NUM + " buckets");
    }

    /**
     * Static-mode counterpart to {@link #testDynamicShuffleTopology}. Proves that with
     * {@link StaticHashAssignmentSource}, multi-partition input is routed in a
     * partition-aware way (records from each partition reach more than one subtask;
     * each subtask sees records from multiple partitions) without any stats operator
     * or coordinator in the topology.
     */
    @Test
    public void testStaticModePartitionAwareRouting() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(PARALLELISM);
        // No checkpointing needed — there's no coordinator to drive.

        RowType flinkSchema = new RowType(Arrays.asList(
                new RowType.RowField("id", new BigIntType()),
                new RowType.RowField("name", new VarCharType()),
                new RowType.RowField("ds", new VarCharType())
        ));
        List<Column> odpsColumns = Arrays.asList(
                new Column("id", TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.BIGINT)),
                new Column("name", TypeInfoFactory.getVarcharTypeInfo(255)),
                new Column("ds", TypeInfoFactory.getVarcharTypeInfo(255))
        );
        PartitionBucketKeyExtractor keyExtractor = new PartitionBucketKeyExtractor(
                odpsColumns,
                Collections.singletonList("id"),
                Collections.singletonList("ds"),
                BUCKET_NUM,
                Collections.emptyMap(),
                null,
                flinkSchema);

        DataStream<RowData> source = env.addSource(
                new TestSourceFunction(NUM_PARTITIONS, RECORDS_PER_PARTITION))
                .setParallelism(1);

        // No statistics operator in this topology — the StaticHashAssignmentSource needs none.
        DataStream<RowData> shuffled = source
                .partitionCustom(
                        new PartitionBucketPartitioner(new StaticHashAssignmentSource()),
                        keyExtractor);

        shuffled.transform("CollectSink",
                        TypeInformation.of(RowData.class),
                        new CollectSinkOperatorFactory())
                .setParallelism(PARALLELISM);

        env.execute("Static-Mode Partition-Aware Routing Smoke Test");

        // 1. All records delivered.
        int totalCollected = COLLECTED.values().stream().mapToInt(List::size).sum();
        assertEquals(NUM_PARTITIONS * RECORDS_PER_PARTITION, totalCollected);

        // 2. Each partition reaches >1 subtask (proves the partitioner is partition-aware,
        //    not bucket-only — a bucket-only partitioner would route all records of one
        //    partition based purely on the bucketId distribution within the partition).
        Map<String, Set<Integer>> subtasksByPartition = new HashMap<>();
        for (Map.Entry<Integer, List<Tuple3<String, Integer, Integer>>> e : COLLECTED.entrySet()) {
            for (Tuple3<String, Integer, Integer> t : e.getValue()) {
                subtasksByPartition.computeIfAbsent(t.f0, k -> new HashSet<>()).add(e.getKey());
            }
        }
        for (Map.Entry<String, Set<Integer>> e : subtasksByPartition.entrySet()) {
            assertTrue("partition " + e.getKey() + " landed on only " + e.getValue() + " subtasks",
                    e.getValue().size() > 1);
        }

        // 3. Each subtask sees records from >1 partition (proves cross-partition mixing).
        for (Map.Entry<Integer, List<Tuple3<String, Integer, Integer>>> e : COLLECTED.entrySet()) {
            Set<String> partitionsHere = new HashSet<>();
            for (Tuple3<String, Integer, Integer> t : e.getValue()) {
                partitionsHere.add(t.f0);
            }
            assertTrue("subtask " + e.getKey() + " saw only partitions " + partitionsHere,
                    partitionsHere.size() > 1);
        }
    }

    // =====================================================================
    // Test Source
    // =====================================================================

    private static class TestSourceFunction implements SourceFunction<RowData> {
        private static final long serialVersionUID = 1L;
        private final int numPartitions;
        private final int recordsPerPartition;
        private volatile boolean running = true;

        TestSourceFunction(int numPartitions, int recordsPerPartition) {
            this.numPartitions = numPartitions;
            this.recordsPerPartition = recordsPerPartition;
        }

        @Override
        public void run(SourceContext<RowData> ctx) throws Exception {
            for (int p = 0; p < numPartitions && running; p++) {
                String partitionValue = "ds=2024010" + p;
                for (int i = 0; i < recordsPerPartition && running; i++) {
                    GenericRowData row = new GenericRowData(3);
                    row.setField(0, (long) (p * recordsPerPartition + i));
                    row.setField(1, StringData.fromString("name_" + i));
                    row.setField(2, StringData.fromString(partitionValue));
                    ctx.collect(row);
                }
            }
            // Give time for checkpoints to trigger and assignment to propagate
            Thread.sleep(5000);
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    // =====================================================================
    // Collect Sink Operator
    // =====================================================================

    private static class CollectSinkOperatorFactory
            extends AbstractStreamOperatorFactory<RowData>
            implements OneInputStreamOperatorFactory<RowData, RowData> {

        @Override
        public <O extends StreamOperator<RowData>> O createStreamOperator(
                StreamOperatorParameters<RowData> parameters) {
            CollectSinkOperator op = new CollectSinkOperator();
            op.setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
            @SuppressWarnings("unchecked")
            O result = (O) op;
            return result;
        }

        @Override
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return CollectSinkOperator.class;
        }
    }

    private static class CollectSinkOperator
            extends AbstractStreamOperator<RowData>
            implements OneInputStreamOperator<RowData, RowData> {

        private int subtaskIndex;

        @Override
        public void open() throws Exception {
            super.open();
            subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
            COLLECTED.put(subtaskIndex, new CopyOnWriteArrayList<>());
        }

        @Override
        public void processElement(StreamRecord<RowData> element) {
            RowData row = element.getValue();
            String ds = row.getString(2).toString();
            int bucketId = (int) (row.getLong(0) % BUCKET_NUM);
            COLLECTED.get(subtaskIndex).add(Tuple3.of(ds, bucketId, (int) row.getLong(0)));
        }
    }
}
