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
 * Stateless {@link AssignmentSource} that routes by
 * {@code Math.floorMod(key.hashCode(), numPartitions)}.
 *
 * <p>Determinism contract: {@link PartitionBucketKey#hashCode()} composes
 * {@code Objects.hash(partitionSpec, bucketId)} over a JLS-stable {@code String.hashCode()}
 * and a trivial {@code Integer.hashCode()}; {@code Arrays.hashCode(Object[])} (the
 * implementation behind {@code Objects.hash}) is itself spec-pinned. Therefore all
 * upstream subtasks of one running job route any given key to the same subtask, and
 * across JVM restarts on the same JDK family the routing is preserved. A pinned-value
 * test guards against accidental drift if {@link PartitionBucketKey#hashCode()} is ever
 * changed.
 *
 * <p>{@link Math#floorMod(int, int)} (not {@code Math.abs(%)}) is used so the
 * {@code Integer.MIN_VALUE} corner of {@code hashCode()} maps to a non-negative subtask.
 */
public final class StaticHashAssignmentSource implements AssignmentSource {
    private static final long serialVersionUID = 1L;

    @Override
    public int route(PartitionBucketKey key, int numPartitions) {
        return Math.floorMod(key.hashCode(), numPartitions);
    }
}
