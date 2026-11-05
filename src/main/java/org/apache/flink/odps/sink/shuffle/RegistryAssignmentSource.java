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

package org.apache.flink.odps.sink.shuffle;

/**
 * {@link AssignmentSource} backed by the coordinator-broadcast assignment held in
 * {@link PartitionBucketAssignmentRegistry}.
 *
 * <p>Fallback semantics — three cases route via the same {@code floorMod(key.hashCode(), N)}
 * formula as {@link StaticHashAssignmentSource} (kept inline rather than delegating, to
 * avoid introducing a coupling that would obscure intent):
 * <ol>
 *   <li>Empty registry — pre-first-checkpoint or registry not yet populated for this
 *       operator UID.</li>
 *   <li>Key absent from the current assignment — first time the partitioner sees a
 *       newly-arrived {@code (partitionSpec, bucketId)} group; the next coordinator
 *       checkpoint will incorporate it.</li>
 *   <li>Assigned subtask out of range — sink parallelism shrunk on rescale and the old
 *       assignment now points beyond the new range.</li>
 * </ol>
 */
public final class RegistryAssignmentSource implements AssignmentSource {
    private static final long serialVersionUID = 1L;

    private final String operatorUid;

    public RegistryAssignmentSource(String operatorUid) {
        this.operatorUid = operatorUid;
    }

    @Override
    public int route(PartitionBucketKey key, int numPartitions) {
        PartitionBucketAssignment assignment =
                PartitionBucketAssignmentRegistry.getInstance().getAssignment(operatorUid);
        if (!assignment.isEmpty()) {
            int subtask = assignment.getSubtask(key);
            if (subtask >= 0 && subtask < numPartitions) {
                return subtask;
            }
        }
        return Math.floorMod(key.hashCode(), numPartitions);
    }
}
