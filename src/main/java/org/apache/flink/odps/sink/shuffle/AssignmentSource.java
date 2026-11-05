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

import java.io.Serializable;

/**
 * Source of {@code (partitionSpec, bucketId) → subtask index} routing decisions for the
 * MaxCompute primary-key sink shuffle. Implementations are stateless or thread-safe and
 * are serialized as part of {@link PartitionBucketPartitioner}, so they must implement
 * {@link Serializable}.
 *
 * <p>Two impls in this package:
 * <ul>
 *   <li>{@link StaticHashAssignmentSource} — stateless {@code floorMod(key.hashCode(), N)}.
 *       Used when {@code sink.dynamic-shuffle.enable=false}.</li>
 *   <li>{@link RegistryAssignmentSource} — reads the coordinator-broadcast assignment from
 *       {@link PartitionBucketAssignmentRegistry}, falls back to the static hash on miss.
 *       Used when {@code sink.dynamic-shuffle.enable=true}.</li>
 * </ul>
 */
public interface AssignmentSource extends Serializable {
    /**
     * Route a key to a subtask in {@code [0, numPartitions)}.
     *
     * @param key           the routing key
     * @param numPartitions the downstream parallelism (= sink parallelism)
     * @return subtask index
     */
    int route(PartitionBucketKey key, int numPartitions);
}
