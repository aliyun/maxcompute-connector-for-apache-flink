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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Global registry for sharing {@link PartitionBucketAssignment} between the statistics operator
 * and the {@link PartitionBucketPartitioner} within the same JVM (TaskManager).
 * <p>
 * Design rationale:
 * <ul>
 *   <li>The statistics operator receives assignment updates from the coordinator via
 *       {@link PartitionBucketAssignmentEvent}.</li>
 *   <li>The partitioner needs to access the latest assignment to make routing decisions.</li>
 *   <li>Since both the statistics operator and the partitioner run in the same JVM (TaskManager),
 *       they can share state through this registry.</li>
 *   <li>The registry uses {@link AtomicReference} for thread-safe updates.</li>
 * </ul>
 * <p>
 * The registry is keyed by operator ID to support multiple sink operators in the same job.
 * Entries are reference-counted; {@link #release} removes the entry when the last user
 * (typically the last subtask of an operator on this JVM) calls release.
 */
public class PartitionBucketAssignmentRegistry {

    /** Singleton instance */
    private static final PartitionBucketAssignmentRegistry INSTANCE = new PartitionBucketAssignmentRegistry();

    /** Map of operator ID -> ref-counted assignment holder */
    private final ConcurrentHashMap<String, Entry> assignments = new ConcurrentHashMap<>();

    private PartitionBucketAssignmentRegistry() {
    }

    public static PartitionBucketAssignmentRegistry getInstance() {
        return INSTANCE;
    }

    /**
     * Register an operator (increments the refcount). Idempotent per caller.
     */
    public AtomicReference<PartitionBucketAssignment> register(String operatorUid) {
        Entry entry = assignments.compute(operatorUid, (k, existing) -> {
            if (existing == null) {
                return new Entry();
            }
            existing.refCount++;
            return existing;
        });
        return entry.ref;
    }

    /**
     * Decrement the refcount; remove the entry when it hits zero.
     */
    public void release(String operatorUid) {
        assignments.computeIfPresent(operatorUid, (k, existing) -> {
            existing.refCount--;
            return existing.refCount <= 0 ? null : existing;
        });
    }

    /**
     * Update the assignment for a given operator.
     */
    public void updateAssignment(String operatorUid, PartitionBucketAssignment assignment) {
        Entry entry = assignments.get(operatorUid);
        if (entry == null) {
            return;
        }
        AtomicReference<PartitionBucketAssignment> ref = entry.ref;
        PartitionBucketAssignment current;
        do {
            current = ref.get();
            // Only update if the new version is newer
            if (assignment.getVersion() <= current.getVersion()) {
                return;
            }
        } while (!ref.compareAndSet(current, assignment));
    }

    /**
     * Get the current assignment for a given operator.
     */
    public PartitionBucketAssignment getAssignment(String operatorUid) {
        Entry entry = assignments.get(operatorUid);
        return entry != null ? entry.ref.get() : new PartitionBucketAssignment();
    }

    /**
     * Force-remove the assignment for a given operator (for testing).
     */
    public void removeAssignment(String operatorUid) {
        assignments.remove(operatorUid);
    }

    private static final class Entry {
        final AtomicReference<PartitionBucketAssignment> ref =
                new AtomicReference<>(new PartitionBucketAssignment());
        int refCount = 1;
    }
}
