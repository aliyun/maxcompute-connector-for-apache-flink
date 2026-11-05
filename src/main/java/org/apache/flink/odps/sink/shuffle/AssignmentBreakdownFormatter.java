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
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Formats a human-readable breakdown of a partition→subtask assignment for logging.
 *
 * <p>Pure function over its inputs: no I/O, no logger, no shared state. Designed to be
 * called from {@link PartitionBucketShuffleCoordinator#checkpointCoordinator} when a new
 * assignment version is produced.
 */
public final class AssignmentBreakdownFormatter {

    private AssignmentBreakdownFormatter() {
        // utility class
    }

    private static final int TOP_N_PARTITIONS = 10;
    private static final int TOP_N_SUBTASKS = 5;

    public static String format(
            long version,
            Map<PartitionBucketKey, Long> stats,
            Map<PartitionBucketKey, Integer> assignment,
            int sinkParallelism) {
        long totalRecords = 0L;
        Set<String> partitionSpecs = new HashSet<>();
        for (Map.Entry<PartitionBucketKey, Long> e : stats.entrySet()) {
            totalRecords += e.getValue();
            partitionSpecs.add(e.getKey().getPartitionSpec());
        }
        int partitions = partitionSpecs.size();

        StringBuilder sb = new StringBuilder();
        sb.append(String.format(Locale.ROOT,
                "Assignment v%d weight breakdown (totalRecords=%,d, partitions=%d, sinkParallelism=%d):",
                version, totalRecords, partitions, sinkParallelism));

        appendPartitionsSection(sb, stats, totalRecords);
        appendSubtaskSection(sb, stats, assignment, sinkParallelism, totalRecords);

        return sb.toString();
    }

    private static void appendPartitionsSection(
            StringBuilder sb, Map<PartitionBucketKey, Long> stats, long totalRecords) {
        Map<String, Long> byPartition = new HashMap<>();
        for (Map.Entry<PartitionBucketKey, Long> e : stats.entrySet()) {
            byPartition.merge(e.getKey().getPartitionSpec(), e.getValue(), Long::sum);
        }

        List<Map.Entry<String, Long>> sorted = new ArrayList<>(byPartition.entrySet());
        sorted.sort(Comparator.comparingLong(Map.Entry<String, Long>::getValue).reversed());

        sb.append('\n').append("  top partitions by records:");

        int n = Math.min(TOP_N_PARTITIONS, sorted.size());
        for (int i = 0; i < n; i++) {
            Map.Entry<String, Long> e = sorted.get(i);
            String spec = e.getKey().isEmpty() ? "<root>" : e.getKey();
            sb.append('\n').append(String.format(Locale.ROOT,
                    "    %s : %s (%s)", spec, formatCount(e.getValue()),
                    formatPercent(e.getValue(), totalRecords)));
        }
        if (sorted.size() > TOP_N_PARTITIONS) {
            long othersRecords = 0L;
            for (int i = TOP_N_PARTITIONS; i < sorted.size(); i++) {
                othersRecords += sorted.get(i).getValue();
            }
            int othersCount = sorted.size() - TOP_N_PARTITIONS;
            sb.append('\n').append(String.format(Locale.ROOT,
                    "    others: %d partitions, %s (%s)",
                    othersCount, formatCount(othersRecords),
                    formatPercent(othersRecords, totalRecords)));
        }
    }

    private static String formatCount(long v) {
        return String.format(Locale.ROOT, "%,d", v);
    }

    private static String formatPercent(long part, long total) {
        if (total <= 0L) {
            return "n/a";
        }
        return String.format(Locale.ROOT, "%.1f%%", (part * 100.0) / total);
    }

    private static void appendSubtaskSection(
            StringBuilder sb,
            Map<PartitionBucketKey, Long> stats,
            Map<PartitionBucketKey, Integer> assignment,
            int sinkParallelism,
            long totalRecords) {
        long[] loads = new long[sinkParallelism];
        for (Map.Entry<PartitionBucketKey, Integer> e : assignment.entrySet()) {
            int sub = e.getValue();
            if (sub < 0 || sub >= sinkParallelism) {
                continue;
            }
            Long w = stats.get(e.getKey());
            if (w != null) {
                loads[sub] += w;
            }
        }

        long avg = sinkParallelism > 0 ? totalRecords / sinkParallelism : 0L;
        long max = 0L;
        for (long l : loads) {
            if (l > max) {
                max = l;
            }
        }
        String skew = avg > 0
                ? String.format(Locale.ROOT, "%.2fx", (double) max / (double) avg)
                : "n/a";

        sb.append('\n').append(String.format(Locale.ROOT,
                "  subtask load (avg=%s skew(max/avg)=%s):", formatCount(avg), skew));

        int idxWidth = String.valueOf(Math.max(0, sinkParallelism - 1)).length();

        if (sinkParallelism <= TOP_N_SUBTASKS * 2) {
            sb.append('\n').append("    all subtasks by load:");
            Integer[] order = sortedIndices(loads, /* descending = */ true);
            for (int idx : order) {
                appendSubtaskRow(sb, idx, loads[idx], totalRecords, idxWidth);
            }
        } else {
            sb.append('\n').append("    top ").append(TOP_N_SUBTASKS).append(" by load:");
            Integer[] desc = sortedIndices(loads, true);
            for (int i = 0; i < TOP_N_SUBTASKS; i++) {
                int idx = desc[i];
                appendSubtaskRow(sb, idx, loads[idx], totalRecords, idxWidth);
            }
            sb.append('\n').append(String.format(Locale.ROOT,
                    "    ... %d more ...", sinkParallelism - 2 * TOP_N_SUBTASKS));
            sb.append('\n').append("    bottom ").append(TOP_N_SUBTASKS).append(" by load:");
            Integer[] asc = sortedIndices(loads, false);
            for (int i = 0; i < TOP_N_SUBTASKS; i++) {
                int idx = asc[i];
                appendSubtaskRow(sb, idx, loads[idx], totalRecords, idxWidth);
            }
        }
    }

    private static Integer[] sortedIndices(long[] loads, boolean descending) {
        Integer[] order = new Integer[loads.length];
        for (int i = 0; i < loads.length; i++) {
            order[i] = i;
        }
        Comparator<Integer> byLoad = Comparator.comparingLong((Integer i) -> loads[i]);
        Comparator<Integer> primary = descending ? byLoad.reversed() : byLoad;
        // Tiebreak: lower subtask index first within the section.
        Arrays.sort(order, primary.thenComparingInt(Integer::intValue));
        return order;
    }

    private static void appendSubtaskRow(
            StringBuilder sb, int idx, long load, long totalRecords, int idxWidth) {
        String idxToken = String.format(Locale.ROOT, "#%-" + idxWidth + "d", idx);
        sb.append('\n').append(String.format(Locale.ROOT,
                "      %s : %s (%s)",
                idxToken, formatCount(load), formatPercent(load, totalRecords)));
    }
}
