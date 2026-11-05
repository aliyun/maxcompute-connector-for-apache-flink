/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * Modified for the MaxCompute Flink connector; see NOTICE for upstream attribution.
 */

package org.apache.flink.odps.sink.common;

import com.aliyun.odps.table.TableIdentifier;
import org.apache.flink.odps.sink.insert.InsertCoordinator;
import org.apache.flink.odps.sink.insert.FileCachedInsertCoordinator;
import org.apache.flink.odps.sink.upsert.FileCachedUpsertCoordinator;
import org.apache.flink.odps.sink.upsert.UpsertCoordinator;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;

public class OdpsWriteCoordinatorProvider implements OperatorCoordinator.Provider {

    private final String operatorName;
    private final OperatorID operatorId;
    private final OdpsWriteOperatorFactory.OdpsWriteOperatorFactoryBuilder builder;

    public OdpsWriteCoordinatorProvider(String operatorName,
                                        OperatorID operatorId,
                                        OdpsWriteOperatorFactory.OdpsWriteOperatorFactoryBuilder builder) {
        this.operatorName = operatorName;
        this.operatorId = operatorId;
        this.builder = builder;
    }

    @Override
    public OperatorID getOperatorId() {
        return this.operatorId;
    }

    @Override
    public OperatorCoordinator create(OperatorCoordinator.Context context) {
        if (builder.getWriteOperationType().equals(WriteOperationType.UPSERT)) {
            if (builder.isFileCached()) {
                return new FileCachedUpsertCoordinator(
                        operatorName,
                        builder.getConf(),
                        context,
                        builder.getOdpsConf(),
                        TableIdentifier.of(
                                builder.getProjectName(), builder.getSchemaName(), builder.getTableName()),
                        builder.getPartition(),
                        builder.getDataSchema(),
                        builder.isDynamicPartition(),
                        builder.isGroupPartition(),
                        builder.getWriteOptions());
            }

            return new UpsertCoordinator(
                    operatorName,
                    builder.getConf(),
                    context,
                    builder.getOdpsConf(),
                    TableIdentifier.of(
                            builder.getProjectName(), builder.getSchemaName(), builder.getTableName()),
                    builder.getPartition(),
                    builder.getDataSchema(),
                    builder.isDynamicPartition(),
                    builder.isGroupPartition(),
                    builder.isSupportPartitionGrouping(),
                    builder.getWriteOptions());

        } else if (builder.getWriteOperationType().equals(WriteOperationType.INSERT)) {
            if (builder.isFileCached()) {
                return new FileCachedInsertCoordinator(
                        operatorName,
                        builder.getConf(),
                        context,
                        builder.getOdpsConf(),
                        TableIdentifier.of(
                                builder.getProjectName(), builder.getSchemaName(), builder.getTableName()),
                        builder.getPartition(),
                        builder.getDataSchema(),
                        builder.isDynamicPartition(),
                        builder.isGroupPartition(),
                        builder.getWriteOptions());
            }

            return new InsertCoordinator(
                    operatorName,
                    builder.getConf(),
                    context,
                    builder.getOdpsConf(),
                    TableIdentifier.of(
                            builder.getProjectName(), builder.getSchemaName(), builder.getTableName()),
                    builder.getPartition(),
                    builder.getDataSchema(),
                    builder.isDynamicPartition(),
                    builder.isGroupPartition(),
                    builder.isSupportPartitionGrouping(),
                    builder.getWriteOptions());
        } else {
            throw new UnsupportedOperationException
                    ("Unsupported write operation type: " + builder.getWriteOperationType().toString());
        }
    }
}
