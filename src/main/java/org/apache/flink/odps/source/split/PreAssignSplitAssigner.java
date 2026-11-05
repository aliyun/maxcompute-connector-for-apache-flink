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

package org.apache.flink.odps.source.split;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class PreAssignSplitAssigner implements SplitAssigner {

    private final int splitBatchSize;

    private final int parallelism;

    private final Map<Integer, LinkedList<OdpsSourceSplit>> pendingSplitAssignment;

    private final AtomicInteger numberOfPendingSplits;

    private final Collection<OdpsSourceSplit> splits;

    public PreAssignSplitAssigner(
            int splitBatchSize, int parallelism, Collection<OdpsSourceSplit> splits) {
        this.splitBatchSize = splitBatchSize;
        this.parallelism = parallelism;
        this.splits = splits;
        this.numberOfPendingSplits = new AtomicInteger(splits.size());
        this.pendingSplitAssignment = new HashMap<>();
    }

    @Override
    public List<OdpsSourceSplit> getNext(int subtask) {
        Queue<OdpsSourceSplit> taskSplits = pendingSplitAssignment.get(subtask);
        List<OdpsSourceSplit> assignment = new ArrayList<>();
        while (taskSplits != null && !taskSplits.isEmpty() && assignment.size() < splitBatchSize) {
            assignment.add(taskSplits.poll());
        }
        numberOfPendingSplits.getAndAdd(-assignment.size());
        return assignment;
    }

    @Override
    public void addSplit(int suggestedTask, OdpsSourceSplit split) {
        pendingSplitAssignment.computeIfAbsent(suggestedTask, k -> new LinkedList<>()).add(split);
        numberOfPendingSplits.incrementAndGet();
    }

    @Override
    public void addSplitsBack(int subtask, List<OdpsSourceSplit> splits) {
        LinkedList<OdpsSourceSplit> remainingSplits =
                pendingSplitAssignment.computeIfAbsent(subtask, k -> new LinkedList<>());
        ListIterator<OdpsSourceSplit> iterator = splits.listIterator(splits.size());
        while (iterator.hasPrevious()) {
            remainingSplits.addFirst(iterator.previous());
        }
        numberOfPendingSplits.getAndAdd(splits.size());
    }

    @Override
    public Collection<OdpsSourceSplit> remainingSplits() {
        List<OdpsSourceSplit> splits = new ArrayList<>();
        pendingSplitAssignment.values().forEach(splits::addAll);
        return splits;
    }

    @Override
    public Optional<Long> getNextSnapshotId(int subtask) {
        LinkedList<OdpsSourceSplit> pendingSplits = pendingSplitAssignment.get(subtask);
//        return (pendingSplits == null || pendingSplits.isEmpty())
//                ? Optional.empty()
//                : getSnapshotId(pendingSplits.peekFirst());
        return Optional.empty();
    }

    @Override
    public int numberOfRemainingSplits() {
        return numberOfPendingSplits.get();
    }
}