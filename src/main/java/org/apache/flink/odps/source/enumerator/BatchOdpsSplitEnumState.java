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

package org.apache.flink.odps.source.enumerator;


import org.apache.flink.odps.source.split.OdpsSourceSplit;

import java.util.LinkedList;
import java.util.List;

/** This state is for {@link BatchOdpsSplitEnumerator} in case of JM failover. */
public class BatchOdpsSplitEnumState {

    private final List<LinkedList<OdpsSourceSplit>> assignedSplits;

    public BatchOdpsSplitEnumState(List<LinkedList<OdpsSourceSplit>> assignedSplits) {
        this.assignedSplits = assignedSplits;
    }

    public List<LinkedList<OdpsSourceSplit>> getAssignedSplits() {
        return assignedSplits;
    }
}
