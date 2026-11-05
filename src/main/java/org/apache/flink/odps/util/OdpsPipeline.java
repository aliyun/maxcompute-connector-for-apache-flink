/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.odps.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.odps.FlinkOdpsException;
import org.apache.flink.odps.sink.common.WriteOperationType;
import org.apache.flink.odps.table.OdpsOptions;
import org.apache.flink.odps.table.OdpsTablePath;
import org.apache.flink.odps.table.factories.OdpsDynamicTableFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class OdpsPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(OdpsPipeline.class);

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private OdpsConf odpsConf;
        private String projectName;
        private String tableName;
        private String schemaName;
        private String partition = "";
        private Configuration conf = new Configuration();

        public Builder() {
        }

        public Builder projectName(String projectName) {
            this.projectName = projectName;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder schemaName(String schemaName) {
            this.schemaName = schemaName;
            return this;
        }

        public Builder partition(String partition) {
            this.partition = partition;
            return this;
        }

        public Builder configuration(Configuration conf) {
            this.conf = conf;
            return this;
        }

        public Builder odpsConf(OdpsConf odpsConf) {
            this.odpsConf = odpsConf;
            return this;
        }

        public TableDescriptor getTableDescriptor(String tableName, CatalogTable table) {
            EnvironmentSettings environmentSettings = EnvironmentSettings
                    .newInstance()
                    .build();
            TableEnvironmentImpl tableEnv = TableEnvironmentImpl.create(environmentSettings);
            String currentCatalog = tableEnv.getCurrentCatalog();
            ResolvedCatalogTable catalogTable = null;
            String defaultDatabase = null;
            try {
                Catalog catalog = tableEnv.getCatalog(currentCatalog).get();
                defaultDatabase = catalog.getDefaultDatabase();
                ObjectPath objectPath = new ObjectPath(defaultDatabase, tableName);
                catalog.createTable(objectPath, table, true);
                catalogTable = (ResolvedCatalogTable) catalog.getTable(objectPath);
            } catch (TableNotExistException | TableAlreadyExistException | DatabaseNotExistException e) {
                throw new FlinkOdpsException("Create table " + this.tableName + " exception", e);
            }
            ObjectIdentifier tableId = ObjectIdentifier.of(currentCatalog, defaultDatabase, tableName);
            return new TableDescriptor(tableId, catalogTable);
        }

        public static FactoryUtil.DefaultDynamicTableContext getTableContext(
                ObjectIdentifier tablePath,
                ResolvedCatalogTable catalogTable,
                ReadableConfig conf) {
            return new FactoryUtil.DefaultDynamicTableContext(tablePath, catalogTable, Collections.emptyMap(),
                    conf, Thread.currentThread().getContextClassLoader(), false);
        }

        public DataStreamSink<?> sink(DataStream<RowData> input, boolean bounded) {
            checkNotNull(odpsConf, "Odps conf should not be null");
            checkNotNull(tableName, "Table name should not be null");

            if (projectName == null) {
                projectName = odpsConf.getProject();
            }

            conf.set(OdpsOptions.PROJECT_NAME, odpsConf.getProject());
            conf.set(OdpsOptions.ACCESS_ID, odpsConf.getAccessId());
            conf.set(OdpsOptions.ACCESS_KEY, odpsConf.getAccessKey());
            conf.set(OdpsOptions.END_POINT, odpsConf.getEndpoint());
            conf.set(OdpsOptions.TUNNEL_END_POINT, odpsConf.getTunnelEndpoint());
            if (odpsConf.getRegionId() != null) {
                conf.set(OdpsOptions.REGION_ID, odpsConf.getRegionId());
            }
            if (odpsConf.getSignatureCorporation() != null) {
                conf.set(OdpsOptions.SIGNATURE_CORPORATION, odpsConf.getSignatureCorporation());
            }
            Odps odps = OdpsUtils.getOdps(odpsConf);
            Table odpsTable;

            if (StringUtils.isNullOrWhitespaceOnly(schemaName)) {
                odpsTable = odps.tables().get(projectName, tableName);
                conf.set(OdpsOptions.TABLE_PATH, OdpsTablePath.toTablePath(projectName, tableName));
            } else {
                conf.set(OdpsOptions.TABLE_PATH, OdpsTablePath.toTablePath(projectName, schemaName, tableName));
                odpsTable = odps.tables().get(projectName, schemaName, tableName);
            }
            List<String> primaryKeys = odpsTable.getPrimaryKey();
            if (primaryKeys != null && !primaryKeys.isEmpty()) {
                int bucketNum = (int)odpsTable.getClusterInfo().getBucketNum();
                conf.set(OdpsOptions.SINK_OPERATION, WriteOperationType.UPSERT.toString());
                conf.set(OdpsOptions.UPSERT_WRITE_BUCKET_NUM, bucketNum);
            }

            TableSchema odpsTableSchema = odpsTable.getSchema();

            // Table schema
            List<Column> fields = odpsTableSchema.getColumns();
            ResolvedSchema flinkTableSchema =
                    OdpsTableUtil.createResolvedSchema(fields, odpsTableSchema.getPartitionColumns(), primaryKeys);

            // Partition keys
            List<String> partitionKeys = new ArrayList<>();
            if (odpsTableSchema.getPartitionColumns().size() > 0) {
                partitionKeys = odpsTableSchema.getPartitionColumns()
                        .stream()
                        .map(Column::getName)
                        .collect(Collectors.toList());

                if (!StringUtils.isNullOrWhitespaceOnly(partition)) {
                    conf.set(OdpsOptions.SINK_PARTITION, partition);
                }
            }

            final CatalogTable origin =
                    CatalogTable.of(
                            Schema.newBuilder().fromResolvedSchema(flinkTableSchema).build(),
                            "",
                            partitionKeys,
                            conf.toMap());
            CatalogTable catalogTable =  new ResolvedCatalogTable(origin, flinkTableSchema);
            TableDescriptor descriptor = getTableDescriptor(tableName, catalogTable);
            FactoryUtil.DefaultDynamicTableContext context = getTableContext(descriptor.getTableId(), descriptor.getResolvedCatalogTable(), Configuration.fromMap(catalogTable.getOptions()));
            OdpsDynamicTableFactory odpsDynamicTableFactory = new OdpsDynamicTableFactory(odpsConf);
            return ((DataStreamSinkProvider) odpsDynamicTableFactory.createDynamicTableSink(context)
                    .getSinkRuntimeProvider(new SinkRuntimeProviderContext(bounded)))
                    .consumeDataStream(input);
        }
    }

    /***
     *  A POJO that contains tableId and resolvedCatalogTable.
     */
    public static class TableDescriptor {
        private final ObjectIdentifier tableId;
        private final ResolvedCatalogTable resolvedCatalogTable;

        public TableDescriptor(ObjectIdentifier tableId, ResolvedCatalogTable resolvedCatalogTable) {
            this.tableId = tableId;
            this.resolvedCatalogTable = resolvedCatalogTable;
        }

        public ObjectIdentifier getTableId() {
            return tableId;
        }

        public ResolvedCatalogTable getResolvedCatalogTable() {
            return resolvedCatalogTable;
        }
    }
}
