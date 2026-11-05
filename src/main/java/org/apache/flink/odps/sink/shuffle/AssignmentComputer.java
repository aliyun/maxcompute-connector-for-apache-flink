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

package org.apache.flink.odps.sink.shuffle;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Computes a balanced assignment of (partitionSpec, bucketId) groups to subtasks.
 * <p>
 * The algorithm is a greedy "first-fit decreasing" bin packing:
 * 1. Sort all (partitionSpec, bucketId) groups by traffic (record count) in descending order.
 * 2. For each group, assign it to the subtask with the current lowest total traffic.
 * <p>
 * Key invariant: each (partitionSpec, bucketId) group is assigned to exactly one subtask.
 * This is the fundamental difference from Fluss's WeightedBucketIdAssignment, which may
 * split the same bucketId across multiple subtasks (because it uses Random within weight ranges).
 * For MaxCompute primary-key tables, splitting is NOT allowed.
 */
public class AssignmentComputer {

    /**
     * Compute a new assignment based on aggregated traffic statistics.
     *
     * @param aggregatedStats global (partitionSpec, bucketId) -> record count
     * @param numSubtasks     number of available subtasks
     * @return assignment mapping (partitionSpec, bucketId) -> subtask index
     */
    public static Map<PartitionBucketKey, Integer> compute(
            Map<PartitionBucketKey, Long> aggregatedStats,
            int numSubtasks) {

        if (numSubtasks <= 0) {
            throw new IllegalArgumentException("numSubtasks must be positive, got: " + numSubtasks);
        }

        // Sort keys by traffic descending
        List<Map.Entry<PartitionBucketKey, Long>> sorted = new ArrayList<>(aggregatedStats.entrySet());
        sorted.sort(Map.Entry.<PartitionBucketKey, Long>comparingByValue().reversed());

        // Track current load per subtask
        long[] subtaskLoads = new long[numSubtasks];

        // Result assignment
        Map<PartitionBucketKey, Integer> assignment = new LinkedHashMap<>();

        for (Map.Entry<PartitionBucketKey, Long> entry : sorted) {
            PartitionBucketKey key = entry.getKey();
            long count = entry.getValue();

            // Find subtask with minimum load (greedy first-fit decreasing)
            int minSubtask = 0;
            long minLoad = subtaskLoads[0];
            for (int i = 1; i < numSubtasks; i++) {
                if (subtaskLoads[i] < minLoad) {
                    minLoad = subtaskLoads[i];
                    minSubtask = i;
                }
            }

            assignment.put(key, minSubtask);
            subtaskLoads[minSubtask] += count;
        }

        return assignment;
    }
}
