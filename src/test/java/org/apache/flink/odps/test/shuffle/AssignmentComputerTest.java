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

import org.apache.flink.odps.sink.shuffle.AssignmentComputer;
import org.apache.flink.odps.sink.shuffle.PartitionBucketKey;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.*;

/**
 * Tests for {@link AssignmentComputer}.
 * <p>
 * The core guarantee: each (partitionSpec, bucketId) key is assigned to exactly one subtask.
 * The greedy bin-packing should produce a reasonably balanced assignment.
 */
public class AssignmentComputerTest {

    // =====================================================================
    // 1. Correctness: each key assigned to exactly one subtask
    // =====================================================================

    @Test
    public void testEveryKeyAssignedExactlyOnce() {
        Map<PartitionBucketKey, Long> stats = new HashMap<>();
        // 5 partitions × 16 buckets = 80 keys
        for (int p = 0; p < 5; p++) {
            for (int b = 0; b < 16; b++) {
                stats.put(new PartitionBucketKey("ds=" + p, b), (long) (p * 16 + b + 1));
            }
        }

        int numSubtasks = 4;
        Map<PartitionBucketKey, Integer> assignment = AssignmentComputer.compute(stats, numSubtasks);

        // Every key must be assigned
        assertEquals(stats.size(), assignment.size());
        for (PartitionBucketKey key : stats.keySet()) {
            assertTrue("Key " + key + " not assigned", assignment.containsKey(key));
            int subtask = assignment.get(key);
            assertTrue("Subtask " + subtask + " out of range",
                    subtask >= 0 && subtask < numSubtasks);
        }

        // No key assigned to multiple subtasks (guaranteed by Map semantics)
    }

    // =====================================================================
    // 2. Deterministic: same input → same output
    // =====================================================================

    @Test
    public void testDeterministicAssignment() {
        Map<PartitionBucketKey, Long> stats = createUniformStats(3, 16, 100L);
        Map<PartitionBucketKey, Integer> a1 = AssignmentComputer.compute(stats, 4);
        Map<PartitionBucketKey, Integer> a2 = AssignmentComputer.compute(stats, 4);
        assertEquals(a1, a2);
    }

    // =====================================================================
    // 3. Balance: uniform traffic → roughly equal load per subtask
    // =====================================================================

    @Test
    public void testBalancedAssignmentUniformTraffic() {
        int numPartitions = 10;
        int numBuckets = 16;
        int numSubtasks = 4;
        long countPerKey = 1000L;

        Map<PartitionBucketKey, Long> stats = createUniformStats(numPartitions, numBuckets, countPerKey);
        Map<PartitionBucketKey, Integer> assignment = AssignmentComputer.compute(stats, numSubtasks);

        // Count total traffic per subtask
        long[] subtaskTraffic = new long[numSubtasks];
        for (Map.Entry<PartitionBucketKey, Integer> entry : assignment.entrySet()) {
            subtaskTraffic[entry.getValue()] += stats.get(entry.getKey());
        }

        long totalTraffic = numPartitions * numBuckets * countPerKey;
        long expectedPerSubtask = totalTraffic / numSubtasks;

        // Each subtask should be within 20% of expected
        for (int i = 0; i < numSubtasks; i++) {
            double ratio = (double) subtaskTraffic[i] / expectedPerSubtask;
            assertTrue("Subtask " + i + " traffic ratio " + ratio + " is out of [0.8, 1.2]",
                    ratio >= 0.8 && ratio <= 1.2);
        }
    }

    // =====================================================================
    // 4. Balance: skewed traffic → greedy still balances reasonably
    // =====================================================================

    @Test
    public void testBalancedAssignmentSkewedTraffic() {
        Map<PartitionBucketKey, Long> stats = new HashMap<>();
        // One hot partition with 10000 per bucket
        for (int b = 0; b < 16; b++) {
            stats.put(new PartitionBucketKey("ds=hot", b), 10000L);
        }
        // Many cold partitions with 10 per bucket
        for (int p = 0; p < 100; p++) {
            for (int b = 0; b < 16; b++) {
                stats.put(new PartitionBucketKey("ds=cold" + p, b), 10L);
            }
        }

        int numSubtasks = 4;
        Map<PartitionBucketKey, Integer> assignment = AssignmentComputer.compute(stats, numSubtasks);

        // Count total traffic per subtask
        long[] subtaskTraffic = new long[numSubtasks];
        for (Map.Entry<PartitionBucketKey, Integer> entry : assignment.entrySet()) {
            subtaskTraffic[entry.getValue()] += stats.get(entry.getKey());
        }

        long totalTraffic = 16 * 10000L + 100 * 16 * 10L;
        long expectedPerSubtask = totalTraffic / numSubtasks;

        // With greedy bin-packing, imbalance should be < 30%
        for (int i = 0; i < numSubtasks; i++) {
            double ratio = (double) subtaskTraffic[i] / expectedPerSubtask;
            assertTrue("Subtask " + i + " traffic ratio " + ratio + " is out of [0.7, 1.3]",
                    ratio >= 0.7 && ratio <= 1.3);
        }
    }

    // =====================================================================
    // 5. Key integrity: same (partitionSpec, bucketId) is never split
    // =====================================================================

    @Test
    public void testKeyNeverSplit() {
        // All data for one (partitionSpec, bucketId) must go to same subtask
        Map<PartitionBucketKey, Long> stats = new HashMap<>();
        stats.put(new PartitionBucketKey("ds=2024", 0), 1_000_000L);
        stats.put(new PartitionBucketKey("ds=2024", 1), 1L);
        stats.put(new PartitionBucketKey("ds=2024", 2), 1L);

        Map<PartitionBucketKey, Integer> assignment = AssignmentComputer.compute(stats, 3);

        // The huge key goes to one subtask, the small keys spread out
        // Each key is assigned to exactly one subtask
        Set<Integer> assignedSubtasks = new TreeSet<>(assignment.values());
        assertTrue(assignedSubtasks.size() <= 3);
        assertTrue(assignedSubtasks.size() >= 1);

        // Verify each key has exactly one assignment
        for (PartitionBucketKey key : stats.keySet()) {
            assertTrue(assignment.containsKey(key));
            assertNotNull(assignment.get(key));
        }
    }

    // =====================================================================
    // 6. Edge case: single subtask
    // =====================================================================

    @Test
    public void testSingleSubtask() {
        Map<PartitionBucketKey, Long> stats = createUniformStats(5, 16, 100L);
        Map<PartitionBucketKey, Integer> assignment = AssignmentComputer.compute(stats, 1);

        // All keys should be assigned to subtask 0
        for (int subtask : assignment.values()) {
            assertEquals(0, subtask);
        }
    }

    // =====================================================================
    // 7. Edge case: more subtasks than keys
    // =====================================================================

    @Test
    public void testMoreSubtasksThanKeys() {
        Map<PartitionBucketKey, Long> stats = new HashMap<>();
        stats.put(new PartitionBucketKey("ds=2024", 0), 100L);
        stats.put(new PartitionBucketKey("ds=2024", 1), 200L);

        Map<PartitionBucketKey, Integer> assignment = AssignmentComputer.compute(stats, 10);

        // Each key assigned to a valid subtask
        for (Map.Entry<PartitionBucketKey, Integer> entry : assignment.entrySet()) {
            assertTrue(entry.getValue() >= 0 && entry.getValue() < 10);
        }

        // At most 2 subtasks are used
        Set<Integer> usedSubtasks = new TreeSet<>(assignment.values());
        assertTrue(usedSubtasks.size() <= 2);
    }

    // =====================================================================
    // 8. Edge case: empty stats
    // =====================================================================

    @Test
    public void testEmptyStats() {
        Map<PartitionBucketKey, Long> stats = new HashMap<>();
        Map<PartitionBucketKey, Integer> assignment = AssignmentComputer.compute(stats, 4);
        assertTrue(assignment.isEmpty());
    }

    // =====================================================================
    // 9. Real-world scale: 500 partitions × 16 buckets, 8 subtasks
    // =====================================================================

    @Test
    public void testRealWorldScale() {
        int numPartitions = 500;
        int numBuckets = 16;
        int numSubtasks = 8;

        Map<PartitionBucketKey, Long> stats = new HashMap<>();
        // Most partitions have similar traffic
        for (int p = 0; p < numPartitions; p++) {
            long base = (p < 10) ? 10000 : 100; // 10 hot partitions
            for (int b = 0; b < numBuckets; b++) {
                stats.put(new PartitionBucketKey("ds=" + p, b), base + b);
            }
        }

        Map<PartitionBucketKey, Integer> assignment = AssignmentComputer.compute(stats, numSubtasks);

        // All keys assigned
        assertEquals(numPartitions * numBuckets, assignment.size());

        // Count traffic per subtask
        long[] subtaskTraffic = new long[numSubtasks];
        int[] subtaskKeys = new int[numSubtasks];
        for (Map.Entry<PartitionBucketKey, Integer> entry : assignment.entrySet()) {
            subtaskTraffic[entry.getValue()] += stats.get(entry.getKey());
            subtaskKeys[entry.getValue()]++;
        }

        // Print distribution for debugging
        long totalTraffic = 0;
        for (long t : subtaskTraffic) totalTraffic += t;
        for (int i = 0; i < numSubtasks; i++) {
            double pct = 100.0 * subtaskTraffic[i] / totalTraffic;
            System.out.printf("  Subtask %d: %d keys, %d traffic (%.1f%%)%n",
                    i, subtaskKeys[i], subtaskTraffic[i], pct);
        }

        // Imbalance ratio: max / min traffic should be < 2.0
        long maxTraffic = 0, minTraffic = Long.MAX_VALUE;
        for (long t : subtaskTraffic) {
            maxTraffic = Math.max(maxTraffic, t);
            minTraffic = Math.min(minTraffic, t);
        }
        double imbalance = (double) maxTraffic / minTraffic;
        System.out.printf("  Imbalance ratio (max/min): %.2f%n", imbalance);
        assertTrue("Imbalance ratio too high: " + imbalance, imbalance < 2.0);
    }

    // =====================================================================
    // 10. Same bucketId across different partitions can go to different subtasks
    // =====================================================================

    @Test
    public void testSameBucketDifferentPartitionDifferentSubtask() {
        Map<PartitionBucketKey, Long> stats = new HashMap<>();
        // 100 partitions, only bucket 0
        for (int p = 0; p < 100; p++) {
            stats.put(new PartitionBucketKey("ds=" + p, 0), 100L);
        }

        int numSubtasks = 8;
        Map<PartitionBucketKey, Integer> assignment = AssignmentComputer.compute(stats, numSubtasks);

        // Same bucket ID (0) across partitions should be spread across subtasks
        Set<Integer> subtasksUsed = new TreeSet<>(assignment.values());
        assertTrue("Same bucket across partitions should use multiple subtasks",
                subtasksUsed.size() > 1);
    }

    // =====================================================================
    // Helpers
    // =====================================================================

    private static Map<PartitionBucketKey, Long> createUniformStats(
            int numPartitions, int numBuckets, long countPerKey) {
        Map<PartitionBucketKey, Long> stats = new HashMap<>();
        for (int p = 0; p < numPartitions; p++) {
            for (int b = 0; b < numBuckets; b++) {
                stats.put(new PartitionBucketKey("ds=" + p, b), countPerKey);
            }
        }
        return stats;
    }
}
