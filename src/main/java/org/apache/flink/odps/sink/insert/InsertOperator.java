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
package org.apache.flink.odps.sink.insert;

import com.aliyun.odps.table.TableIdentifier;
import org.apache.flink.odps.sink.common.AbstractWriteOperator;
import org.apache.flink.odps.sink.common.OdpsWriteOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.table.data.RowData;

/**
 * Operator for {@link StreamSink}.
 */
public class InsertOperator extends AbstractWriteOperator<RowData> {

    public InsertOperator(OdpsWriteOperatorFactory.OdpsWriteOperatorFactoryBuilder builder) {
        super(new InsertFunction(builder.getConf(),
                builder.getOdpsConf(),
                TableIdentifier.of(builder.getProjectName(), builder.getSchemaName(), builder.getTableName()),
                builder.getPartition(),
                builder.getDataSchema(),
                builder.isDynamicPartition(),
                builder.isGroupPartition(),
                builder.isSupportPartitionGrouping(),
                builder.getWriteOptions(),
                builder.getPartitionAssigner(),
                builder.getWriteOperationType(),
                builder.getRowType()));
    }
}
