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

import org.apache.flink.runtime.operators.coordination.OperatorEvent;

/**
 * Event carrying local statistics from a subtask to the coordinator.
 */
public class PartitionBucketStatisticsEvent implements OperatorEvent {

    private static final long serialVersionUID = 1L;

    private final PartitionBucketStatistics statistics;

    public PartitionBucketStatisticsEvent(PartitionBucketStatistics statistics) {
        this.statistics = statistics;
    }

    public PartitionBucketStatistics getStatistics() {
        return statistics;
    }

    @Override
    public String toString() {
        return "PartitionBucketStatisticsEvent{" +
                "statistics=" + statistics +
                '}';
    }
}
