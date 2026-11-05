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

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * The assignment mapping from (partitionSpec, bucketId) to target subtask index.
 *
 * <p>Produced by the coordinator after aggregating global statistics and broadcast to all
 * subtasks. The partitioner uses this mapping to route records.
 *
 * <p>Key invariant: for any (partitionSpec, bucketId), there is exactly ONE target subtask.
 * This is required by MaxCompute's primary-key partitioned tables, where each bucket can
 * only have one writer.
 *
 * <p><b>{@code validFromCheckpointId}</b> is the barrier-alignment token: the subtask must
 * not switch to this assignment until it has processed checkpoint barrier {@code >= validFromCheckpointId}.
 * The statistics operator queues incoming assignments and applies them only at
 * {@code snapshotState(N)} where {@code N >= validFromCheckpointId}, so that all subtasks
 * switch around the same barrier rather than at arbitrary event-delivery times. A value of
 * {@code 0} marks a bootstrap assignment that should be applied immediately (used in
 * {@link PartitionBucketShuffleCoordinator#executionAttemptReady}).
 */
public class PartitionBucketAssignment implements Serializable {

    private static final long serialVersionUID = 2L;

    /** "Apply immediately" sentinel for the bootstrap path. */
    public static final long APPLY_IMMEDIATELY = 0L;

    /** (partitionSpec, bucketId) -> subtask index */
    private final Map<PartitionBucketKey, Integer> assignment;

    /** Version counter incremented on each new assignment, used to detect stale updates */
    private final long version;

    /**
     * The assignment may be applied only after the receiving subtask has processed checkpoint
     * barrier {@code N >= validFromCheckpointId}. {@link #APPLY_IMMEDIATELY} disables the
     * gating (used for bootstrap on subtask restart).
     */
    private final long validFromCheckpointId;

    public PartitionBucketAssignment() {
        this.assignment = Collections.emptyMap();
        this.version = 0;
        this.validFromCheckpointId = APPLY_IMMEDIATELY;
    }

    /** Convenience overload — sets {@code validFromCheckpointId = APPLY_IMMEDIATELY}. */
    public PartitionBucketAssignment(Map<PartitionBucketKey, Integer> assignment, long version) {
        this(assignment, version, APPLY_IMMEDIATELY);
    }

    public PartitionBucketAssignment(
            Map<PartitionBucketKey, Integer> assignment, long version, long validFromCheckpointId) {
        this.assignment = Collections.unmodifiableMap(new HashMap<>(assignment));
        this.version = version;
        this.validFromCheckpointId = validFromCheckpointId;
    }

    /**
     * Returns the target subtask for the given (partitionSpec, bucketId), or -1 if not assigned.
     */
    public int getSubtask(PartitionBucketKey key) {
        return assignment.getOrDefault(key, -1);
    }

    public Map<PartitionBucketKey, Integer> getAssignment() {
        return assignment;
    }

    public long getVersion() {
        return version;
    }

    public long getValidFromCheckpointId() {
        return validFromCheckpointId;
    }

    public boolean isEmpty() {
        return assignment.isEmpty();
    }

    /**
     * Return a copy of this assignment with {@code validFromCheckpointId = APPLY_IMMEDIATELY}.
     * Used by the coordinator when bootstrapping a freshly-ready subtask attempt — the new
     * attempt must catch up to the cluster-wide assignment ASAP, without waiting for its own
     * next checkpoint barrier.
     */
    public PartitionBucketAssignment asBootstrap() {
        if (validFromCheckpointId == APPLY_IMMEDIATELY) {
            return this;
        }
        return new PartitionBucketAssignment(assignment, version, APPLY_IMMEDIATELY);
    }

    @Override
    public String toString() {
        return "PartitionBucketAssignment{" +
                "version=" + version +
                ", validFromCheckpointId=" + validFromCheckpointId +
                ", assignmentSize=" + assignment.size() +
                '}';
    }
}
