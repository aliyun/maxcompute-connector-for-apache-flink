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

import org.apache.flink.odps.sink.shuffle.*;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.*;

/**
 * Integration test simulating the Coordinator + Partitioner flow:
 * 1. Multiple subtasks report statistics via events
 * 2. Coordinator aggregates at checkpoint and computes assignment
 * 3. Partitioner uses the new assignment from Registry
 * <p>
 * Note: We test coordinator logic directly (without SubtaskGateway mock),
 * then verify the partitioner reads the correct assignment from the Registry.
 */
public class ShuffleCoordinatorIntegrationTest {

    @Test
    public void testFullCoordinatorFlow() throws Exception {
        int numSubtasks = 4;
        String operatorUid = "integration-test-op";
        OperatorID operatorID = new OperatorID();

        // Create coordinator
        PartitionBucketShuffleCoordinator coordinator =
                new PartitionBucketShuffleCoordinator(operatorID, numSubtasks, numSubtasks);
        coordinator.start();

        // =====================================================================
        // Step 1: Simulate statistics from 4 subtasks
        // =====================================================================
        PartitionBucketStatistics stats0 = new PartitionBucketStatistics(0);
        stats0.increment(new PartitionBucketKey("ds=2024", 0), 100);
        stats0.increment(new PartitionBucketKey("ds=2024", 1), 200);
        stats0.increment(new PartitionBucketKey("ds=2024", 2), 150);
        stats0.increment(new PartitionBucketKey("ds=2024", 3), 50);
        coordinator.handleEventFromOperator(0, 0, new PartitionBucketStatisticsEvent(stats0));

        PartitionBucketStatistics stats1 = new PartitionBucketStatistics(1);
        stats1.increment(new PartitionBucketKey("ds=2024", 4), 300);
        stats1.increment(new PartitionBucketKey("ds=2024", 5), 100);
        stats1.increment(new PartitionBucketKey("ds=2024", 6), 250);
        stats1.increment(new PartitionBucketKey("ds=2024", 7), 75);
        coordinator.handleEventFromOperator(1, 0, new PartitionBucketStatisticsEvent(stats1));

        PartitionBucketStatistics stats2 = new PartitionBucketStatistics(2);
        stats2.increment(new PartitionBucketKey("ds=2025", 0), 500);
        stats2.increment(new PartitionBucketKey("ds=2025", 1), 50);
        stats2.increment(new PartitionBucketKey("ds=2025", 2), 80);
        stats2.increment(new PartitionBucketKey("ds=2025", 3), 120);
        coordinator.handleEventFromOperator(2, 0, new PartitionBucketStatisticsEvent(stats2));

        PartitionBucketStatistics stats3 = new PartitionBucketStatistics(3);
        stats3.increment(new PartitionBucketKey("ds=2025", 4), 200);
        stats3.increment(new PartitionBucketKey("ds=2025", 5), 400);
        stats3.increment(new PartitionBucketKey("ds=2025", 6), 60);
        stats3.increment(new PartitionBucketKey("ds=2025", 7), 90);
        coordinator.handleEventFromOperator(3, 0, new PartitionBucketStatisticsEvent(stats3));

        // =====================================================================
        // Step 2: Checkpoint triggers assignment computation
        // =====================================================================
        CompletableFuture<byte[]> checkpointFuture = new CompletableFuture<>();
        coordinator.checkpointCoordinator(1L, checkpointFuture);
        assertTrue(checkpointFuture.isDone());

        // =====================================================================
        // Step 3: Verify assignment was computed correctly
        // =====================================================================
        PartitionBucketAssignment assignment = coordinator.getCurrentAssignment();
        assertEquals(1L, assignment.getVersion());
        assertEquals(16, assignment.getAssignment().size());  // 2 partitions × 8 buckets

        // =====================================================================
        // Step 4: Simulate writing assignment to Registry (what the operator would do)
        // =====================================================================
        PartitionBucketAssignmentRegistry.getInstance().register(operatorUid);
        PartitionBucketAssignmentRegistry.getInstance().updateAssignment(operatorUid, assignment);

        PartitionBucketPartitioner partitioner = new PartitionBucketPartitioner(new RegistryAssignmentSource(operatorUid));

        // Every key in the assignment should route to its assigned subtask
        for (Map.Entry<PartitionBucketKey, Integer> entry : assignment.getAssignment().entrySet()) {
            int routed = partitioner.partition(entry.getKey(), numSubtasks);
            assertEquals("Key " + entry.getKey() + " should route to " + entry.getValue() +
                    " but got " + routed, (int) entry.getValue(), routed);
        }

        // =====================================================================
        // Step 5: Verify balance - no subtask should have >50% more traffic than average
        // =====================================================================
        long[] subtaskTraffic = new long[numSubtasks];
        long totalTraffic = 0;
        Map<PartitionBucketKey, Long> allStats = new HashMap<>();
        for (PartitionBucketStatistics s : new PartitionBucketStatistics[]{stats0, stats1, stats2, stats3}) {
            for (Map.Entry<PartitionBucketKey, Long> e : s.getBucketCounts().entrySet()) {
                allStats.merge(e.getKey(), e.getValue(), Long::sum);
            }
        }
        for (Map.Entry<PartitionBucketKey, Integer> entry : assignment.getAssignment().entrySet()) {
            subtaskTraffic[entry.getValue()] += allStats.get(entry.getKey());
            totalTraffic += allStats.get(entry.getKey());
        }

        long avgTraffic = totalTraffic / numSubtasks;
        System.out.println("=== Traffic Distribution ===");
        for (int i = 0; i < numSubtasks; i++) {
            double pct = 100.0 * subtaskTraffic[i] / totalTraffic;
            System.out.printf("  Subtask %d: %d traffic (%.1f%%)%n", i, subtaskTraffic[i], pct);
            // Allow 50% deviation from average
            assertTrue("Subtask " + i + " has too much traffic (" + subtaskTraffic[i] + " vs avg " + avgTraffic + ")",
                    subtaskTraffic[i] <= avgTraffic * 1.5);
        }

        // Cleanup
        PartitionBucketAssignmentRegistry.getInstance().removeAssignment(operatorUid);
        coordinator.close();
    }

    @Test
    public void testAssignmentVersionIncrements() throws Exception {
        int numSubtasks = 2;
        OperatorID operatorID = new OperatorID();
        PartitionBucketShuffleCoordinator coordinator =
                new PartitionBucketShuffleCoordinator(operatorID, numSubtasks, numSubtasks);
        coordinator.start();

        // Checkpoint 1: no stats → no new assignment
        CompletableFuture<byte[]> cp1 = new CompletableFuture<>();
        coordinator.checkpointCoordinator(1L, cp1);
        assertTrue(cp1.isDone());
        assertEquals(0L, coordinator.getCurrentAssignment().getVersion());

        // Add stats and checkpoint 2
        PartitionBucketStatistics stats = new PartitionBucketStatistics(0);
        stats.increment(new PartitionBucketKey("ds=2024", 0), 100);
        coordinator.handleEventFromOperator(0, 0, new PartitionBucketStatisticsEvent(stats));

        CompletableFuture<byte[]> cp2 = new CompletableFuture<>();
        coordinator.checkpointCoordinator(2L, cp2);
        assertTrue(cp2.isDone());
        assertEquals(1L, coordinator.getCurrentAssignment().getVersion());
        assertFalse(coordinator.getCurrentAssignment().isEmpty());

        // Add more stats and checkpoint 3
        stats = new PartitionBucketStatistics(1);
        stats.increment(new PartitionBucketKey("ds=2025", 0), 200);
        coordinator.handleEventFromOperator(1, 0, new PartitionBucketStatisticsEvent(stats));

        CompletableFuture<byte[]> cp3 = new CompletableFuture<>();
        coordinator.checkpointCoordinator(3L, cp3);
        assertTrue(cp3.isDone());
        assertEquals(2L, coordinator.getCurrentAssignment().getVersion());

        coordinator.close();
    }

    @Test
    public void testResetClearsState() throws Exception {
        OperatorID operatorID = new OperatorID();
        PartitionBucketShuffleCoordinator coordinator =
                new PartitionBucketShuffleCoordinator(operatorID, 2, 2);
        coordinator.start();

        // Add stats
        PartitionBucketStatistics stats = new PartitionBucketStatistics(0);
        stats.increment(new PartitionBucketKey("ds=2024", 0), 100);
        coordinator.handleEventFromOperator(0, 0, new PartitionBucketStatisticsEvent(stats));

        // Reset
        coordinator.resetToCheckpoint(1L, new byte[0]);

        // Checkpoint should produce empty assignment (stats were cleared)
        CompletableFuture<byte[]> cp = new CompletableFuture<>();
        coordinator.checkpointCoordinator(2L, cp);
        assertTrue(cp.isDone());
        // Assignment version should not increment since no stats
        assertEquals(0L, coordinator.getCurrentAssignment().getVersion());

        coordinator.close();
    }

    @Test
    public void testStaleAssignmentNotApplied() throws Exception {
        // Verify that an older version assignment doesn't overwrite a newer one
        String operatorUid = "test-stale";

        PartitionBucketAssignmentRegistry.getInstance().register(operatorUid);

        // Set v2 assignment first
        Map<PartitionBucketKey, Integer> map2 = new HashMap<>();
        map2.put(new PartitionBucketKey("ds=2024", 0), 3);
        PartitionBucketAssignmentRegistry.getInstance().updateAssignment(operatorUid,
                new PartitionBucketAssignment(map2, 2L));

        // Try to set v1 assignment (should be rejected)
        Map<PartitionBucketKey, Integer> map1 = new HashMap<>();
        map1.put(new PartitionBucketKey("ds=2024", 0), 0);
        PartitionBucketAssignmentRegistry.getInstance().updateAssignment(operatorUid,
                new PartitionBucketAssignment(map1, 1L));

        // v2 should still be active
        PartitionBucketPartitioner partitioner = new PartitionBucketPartitioner(new RegistryAssignmentSource(operatorUid));
        assertEquals(3, partitioner.partition(new PartitionBucketKey("ds=2024", 0), 4));

        // Cleanup
        PartitionBucketAssignmentRegistry.getInstance().removeAssignment(operatorUid);
    }
}
