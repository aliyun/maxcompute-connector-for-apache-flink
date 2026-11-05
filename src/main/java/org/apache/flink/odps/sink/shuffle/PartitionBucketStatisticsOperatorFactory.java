/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for the regarding copyright ownership.
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

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for {@link PartitionBucketStatisticsOperator}.
 * <p>
 * Implements {@link CoordinatedOperatorFactory} to provide the
 * {@link PartitionBucketShuffleCoordinator} for dynamic assignment management.
 */
public class PartitionBucketStatisticsOperatorFactory
        extends AbstractStreamOperatorFactory<RowData>
        implements CoordinatedOperatorFactory<RowData>, OneInputStreamOperatorFactory<RowData, RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionBucketStatisticsOperatorFactory.class);

    private final PartitionBucketKeyExtractor keyExtractor;
    private final String operatorUid;
    private final int statsParallelism;
    private final int sinkParallelism;

    public PartitionBucketStatisticsOperatorFactory(
            PartitionBucketKeyExtractor keyExtractor,
            String operatorUid,
            int statsParallelism,
            int sinkParallelism) {
        this.keyExtractor = keyExtractor;
        this.operatorUid = operatorUid;
        this.statsParallelism = statsParallelism;
        this.sinkParallelism = sinkParallelism;
    }

    @Override
    public <O extends StreamOperator<RowData>> O createStreamOperator(
            StreamOperatorParameters<RowData> parameters) {

        // Get operator ID and event gateway from the environment
        OperatorID operatorID = parameters.getStreamConfig().getOperatorID();
        OperatorEventDispatcher eventDispatcher = parameters.getOperatorEventDispatcher();
        OperatorEventGateway eventGateway = eventDispatcher.getOperatorEventGateway(operatorID);

        PartitionBucketStatisticsOperator operator =
                new PartitionBucketStatisticsOperator(keyExtractor, eventGateway, operatorUid);

        // Wire up operator event handling
        eventDispatcher.registerEventHandler(operatorID, (event) -> {
            if (event instanceof PartitionBucketAssignmentEvent) {
                operator.handleOperatorEvent((PartitionBucketAssignmentEvent) event);
            }
        });

        operator.setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());

        @SuppressWarnings("unchecked")
        O result = (O) operator;
        return result;
    }

    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(String operatorName, OperatorID operatorID) {
        LOG.info("Creating coordinator provider for operator {} with ID {}, statsParallelism={}, sinkParallelism={}",
                operatorName, operatorID, statsParallelism, sinkParallelism);
        return new PartitionBucketShuffleCoordinatorProvider(operatorID, statsParallelism, sinkParallelism);
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return PartitionBucketStatisticsOperator.class;
    }
}
