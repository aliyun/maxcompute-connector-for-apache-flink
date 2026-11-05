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

import java.util.Collection;
import java.util.List;
import java.util.Optional;

public interface SplitAssigner {

    /**
     * Gets the next split.
     *
     * <p>When this method returns an empty {@code Optional}, then the set of splits is assumed to
     * be done and the source will finish once the readers finished their current splits.
     */
    List<OdpsSourceSplit> getNext(int subtask);

    /** Add one split of a specified subtask to the assigner. */
    void addSplit(int suggestedTask, OdpsSourceSplit splits);

    /**
     * Adds a set of splits to this assigner. This happens for example when some split processing
     * failed and the splits need to be re-added, or when new splits got discovered.
     */
    void addSplitsBack(int subtask, List<OdpsSourceSplit> splits);

    /** Gets the remaining splits that this assigner has pending. */
    Collection<OdpsSourceSplit> remainingSplits();

    /** Gets the snapshot id of the next split. */
    Optional<Long> getNextSnapshotId(int subtask);

    /**
     * Gets the current number of remaining splits. This method should be guaranteed to be
     * thread-safe.
     */
    int numberOfRemainingSplits();
}