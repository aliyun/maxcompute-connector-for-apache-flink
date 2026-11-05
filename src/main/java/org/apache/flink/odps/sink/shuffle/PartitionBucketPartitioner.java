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

import org.apache.flink.api.common.functions.Partitioner;

/**
 * Routes {@link PartitionBucketKey} records to subtasks via an injected
 * {@link AssignmentSource}.
 *
 * <p>Two configured shapes:
 * <ul>
 *   <li>{@link StaticHashAssignmentSource} — used when
 *       {@code sink.dynamic-shuffle.enable=false}. Stateless, deterministic.</li>
 *   <li>{@link RegistryAssignmentSource} — used when
 *       {@code sink.dynamic-shuffle.enable=true}. Reads coordinator-broadcast
 *       assignment with a hash fallback.</li>
 * </ul>
 *
 * <p>Thread safety: both shipped sources read from immutable / atomic state
 * ({@code AtomicReference} inside the registry); the partitioner itself holds no
 * mutable state.
 */
public final class PartitionBucketPartitioner implements Partitioner<PartitionBucketKey> {
    private static final long serialVersionUID = 1L;

    private final AssignmentSource source;

    public PartitionBucketPartitioner(AssignmentSource source) {
        this.source = source;
    }

    @Override
    public int partition(PartitionBucketKey key, int numPartitions) {
        return source.route(key, numPartitions);
    }
}
