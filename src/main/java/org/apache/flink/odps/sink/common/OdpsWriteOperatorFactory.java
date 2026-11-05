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

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.table.DataSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.FlinkOdpsException;
import org.apache.flink.odps.sink.insert.FileCachedInsertOperator;
import org.apache.flink.odps.sink.insert.StandaloneInsertOperator;
import org.apache.flink.odps.sink.partition.PartitionAssigner;
import org.apache.flink.odps.sink.insert.InsertOperator;
import org.apache.flink.odps.sink.upsert.FileCachedUpsertOperator;
import org.apache.flink.odps.sink.upsert.UpsertOperator;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsTableUtil;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class OdpsWriteOperatorFactory extends AbstractWriteOperatorFactory<RowData> {

    private final OdpsWriteOperatorFactoryBuilder builder;

    public OdpsWriteOperatorFactory(Configuration conf,
                                    OdpsWriteOperatorFactoryBuilder builder) {
        super(conf, createOperator(builder));
        this.builder = builder;
    }

    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(String operatorName, OperatorID operatorID) {
        return new OdpsWriteCoordinatorProvider(
                operatorName,
                operatorID,
                builder);
    }

    public static AbstractWriteOperator<RowData> createOperator(OdpsWriteOperatorFactoryBuilder builder) {
        if (builder.writeOperationType.equals(WriteOperationType.UPSERT)) {
            if (builder.isFileCached()) {
                return new FileCachedUpsertOperator(builder);
            }

            return new UpsertOperator(builder);
        } else if (builder.writeOperationType.equals(WriteOperationType.INSERT)) {
            if (builder.isFileCached()) {
                return new FileCachedInsertOperator(builder);
            } else if (builder.isStandalone()) {
                return new StandaloneInsertOperator(builder);
            }
            return new InsertOperator(builder);
        } else {
            throw new UnsupportedOperationException
                    ("Unsupported write operation type: " + builder.getWriteOperationType().toString());
        }
    }

    /**
     * Builder to build {@link OdpsWriteOperatorFactory}.
     */
    public static class OdpsWriteOperatorFactoryBuilder implements Serializable {
        private static final long serialVersionUID = 1L;
        private OdpsConf odpsConf;
        private String projectName;
        private String tableName;
        private String schemaName = "default";
        private String partition;
        private DataSchema dataSchema;
        private WriteOperationType writeOperationType = WriteOperationType.INSERT;
        private boolean isDynamicPartition = false;
        private boolean isGroupPartition = false;
        private boolean isAsyncCommit = false;
        private boolean isFileCached = false;

        private boolean isSupportPartitionGrouping = false;
        private boolean isStandalone = false;
        private PartitionAssigner<RowData> partitionAssigner;
        private Configuration conf;
        private OdpsWriteOptions writeOptions;
        private RowType rowType;

        public OdpsWriteOperatorFactoryBuilder(String projectName, String tableName) {
            this(null, projectName, tableName);
        }

        public OdpsWriteOperatorFactoryBuilder(OdpsConf odpsConf,
                                               String projectName,
                                               String tableName) {
            this(odpsConf, projectName, tableName, null);
        }

        public OdpsWriteOperatorFactoryBuilder(OdpsConf odpsConf,
                                               String projectName,
                                               String tableName,
                                               OdpsWriteOptions writeOptions) {
            this.odpsConf = odpsConf;
            this.projectName = projectName;
            this.tableName = tableName;
            this.writeOptions = writeOptions;
        }

        public OdpsWriteOperatorFactoryBuilder setConf(Configuration conf) {
            this.conf = conf;
            return this;
        }

        public OdpsWriteOperatorFactoryBuilder setOdpsConf(OdpsConf odpsConf) {
            this.odpsConf = odpsConf;
            return this;
        }

        public OdpsWriteOperatorFactoryBuilder setProjectName(String projectName) {
            this.projectName = projectName;
            return this;
        }

        public OdpsWriteOperatorFactoryBuilder setSchemaName(String schemaName) {
            if (!StringUtils.isNullOrWhitespaceOnly(schemaName)) {
                this.schemaName = schemaName;
            }
            return this;
        }

        public OdpsWriteOperatorFactoryBuilder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public OdpsWriteOperatorFactoryBuilder setPartition(String partition) {
            this.partition = partition;
            return this;
        }

        public OdpsWriteOperatorFactoryBuilder setTableSchema(TableSchema schema) {
            List<Column> schemaColumns = new ArrayList<>();
            schemaColumns.addAll(schema.getColumns());
            schemaColumns.addAll(schema.getPartitionColumns());
            this.dataSchema = DataSchema.newBuilder()
                    .columns(schemaColumns)
                    .partitionBy(schema.getPartitionColumns()
                            .stream()
                            .map(Column::getName)
                            .collect(Collectors.toList()))
                    .build();
            return this;
        }

        public OdpsWriteOperatorFactoryBuilder setWriteOperationType(WriteOperationType writeOperationType) {
            this.writeOperationType = writeOperationType;
            return this;
        }

        public OdpsWriteOperatorFactoryBuilder setWriteOptions(OdpsWriteOptions writeOptions) {
            this.writeOptions = writeOptions;
            return this;
        }

        public OdpsWriteOperatorFactoryBuilder setDynamicPartition(boolean dynamicPartition) {
            this.isDynamicPartition = dynamicPartition;
            return this;
        }

        public OdpsWriteOperatorFactoryBuilder setGroupPartition(boolean groupPartition) {
            this.isGroupPartition = groupPartition;
            return this;
        }

        public OdpsWriteOperatorFactoryBuilder setAsyncCommit(boolean asyncCommit) {
            this.isAsyncCommit = asyncCommit;
            return this;
        }

        public OdpsWriteOperatorFactoryBuilder setFileCached(boolean fileCached) {
            this.isFileCached = fileCached;
            return this;
        }

        public OdpsWriteOperatorFactoryBuilder setSupportPartitionGrouping(boolean supportPartitionGrouping) {
            this.isSupportPartitionGrouping = supportPartitionGrouping;
            return this;
        }

        public OdpsWriteOperatorFactoryBuilder setPartitionAssigner(PartitionAssigner<RowData> partitionAssigner) {
            this.partitionAssigner = partitionAssigner;
            return this;
        }

        public OdpsWriteOperatorFactoryBuilder setStandAlone(boolean isStandalone) {
            this.isStandalone = isStandalone;
            return this;
        }

        public OdpsWriteOperatorFactoryBuilder setRowType(RowType rowType) {
            this.rowType = rowType;
            return this;
        }

        public OdpsConf getOdpsConf() {
            return odpsConf;
        }

        public String getPartition() {
            return partition;
        }

        public String getTableName() {
            return tableName;
        }

        public String getSchemaName() {
            return schemaName;
        }

        public String getProjectName() {
            return projectName;
        }

        public Configuration getConf() {
            return conf;
        }

        public OdpsWriteOptions getWriteOptions() {
            return writeOptions;
        }

        public PartitionAssigner<RowData> getPartitionAssigner() {
            return partitionAssigner;
        }

        public WriteOperationType getWriteOperationType() {
            return writeOperationType;
        }

        public DataSchema getDataSchema() {
            return dataSchema;
        }

        public boolean isDynamicPartition() {
            return isDynamicPartition;
        }

        public boolean isGroupPartition() {
            return isGroupPartition;
        }

        public boolean isAsyncCommit() {
            return isAsyncCommit;
        }

        public boolean isFileCached() {
            return isFileCached;
        }

        public boolean isSupportPartitionGrouping() {
            return isSupportPartitionGrouping;
        }

        public boolean isStandalone() {
            return isStandalone;
        }

        public RowType getRowType() {
            return rowType;
        }
        public OdpsWriteOperatorFactory build() {
            checkNotNull(projectName, "projectName should not be null");
            checkNotNull(tableName, "tableName should not be null");

            if (isGroupPartition && !isDynamicPartition) {
                throw new FlinkOdpsException("Dynamic partition should be true");
            }

            if (isAsyncCommit && !isFileCached) {
                throw new FlinkOdpsException("File cached should be true");
            }

            if (dataSchema == null) {
                Odps odps = OdpsUtils.getOdps(odpsConf);
                Table odpsTable = odps.tables().get(odpsConf.getProject(), schemaName, tableName);
                TableSchema tableSchema = odpsTable.getSchema();
                List<Column> schemaColumns = new ArrayList<>();
                schemaColumns.addAll(tableSchema.getColumns());
                schemaColumns.addAll(tableSchema.getPartitionColumns());
                this.dataSchema = DataSchema.newBuilder()
                        .columns(schemaColumns)
                        .partitionBy(tableSchema.getPartitionColumns()
                                .stream()
                                .map(Column::getName)
                                .collect(Collectors.toList()))
                        .build();
            }
            RowType odpsSchema = (RowType)OdpsTableUtil.toRowDataType(dataSchema.getColumns()).getLogicalType();
            if (rowType == null) {
                this.rowType = odpsSchema;
            }
            else {
                OdpsTableUtil.checkPartialSchema(this.rowType, odpsSchema);
            }
            return new OdpsWriteOperatorFactory(conf, this);
        }
    }
}
