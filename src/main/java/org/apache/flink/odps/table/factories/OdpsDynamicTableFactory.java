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

package org.apache.flink.odps.table.factories;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.odps.FlinkOdpsException;
import org.apache.flink.odps.sink.common.WriteOperationType;
import org.apache.flink.odps.source.OdpsLookupOptions;
import org.apache.flink.odps.table.OdpsDynamicTableSink;
import org.apache.flink.odps.table.OdpsDynamicTableSource;
import org.apache.flink.odps.table.OdpsOptions;
import org.apache.flink.odps.table.OdpsTablePath;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.flink.odps.table.OdpsOptions.*;

import com.aliyun.odps.table.TableIdentifier;

/**
 * A dynamic table factory implementation for Odps catalog.
 */
public class OdpsDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    private final OdpsConf odpsConf;
    public static final String IDENTIFIER = "maxcompute";

    public OdpsDynamicTableFactory() {
        this.odpsConf = OdpsUtils.getOdpsConf();
    }

    public OdpsDynamicTableFactory(OdpsConf odpsConf) {
        this.odpsConf = odpsConf;
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(TABLE_PATH);
        return set;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(ACCESS_ID);
        set.add(ACCESS_KEY);
        set.add(PROJECT_NAME);
        set.add(END_POINT);
        set.add(HTTP_PROXY);
        set.add(HTTPS_PROXY);
        set.add(ODPS_PROXY_DISABLE_RESOLVER);
        set.add(TUNNEL_END_POINT);
        set.add(TUNNEL_QUOTA_NAME);
        set.add(SINK_OPERATION);
        set.add(ODPS_NAMESPACE_SCHEMA_ENABLE);
        set.add(REGION_ID);
        set.add(SIGNATURE_CORPORATION);
        
        set.add(CREDENTIALS_PROVIDER);
        set.add(CREDENTIALS_PROVIDER_PARAM);

        set.add(COMPRESS_CODEC);
        set.add(SPLIT_STRATEGY);
        set.add(SPLIT_BYTE_SIZE);
        set.add(SPLIT_ROW_OFFSET_GRANULARITY);
        set.add(PARTITION);
        set.add(DYNAMIC_LOAD_BALANCING);

        set.add(SPLIT_BATCH_READ_SIZE);
        set.add(CONTINUOUS_DISCOVERY_INTERVAL);
        set.add(SCAN_MAX_SPLITS_PER_TASK);

        set.add(SCAN_PARTITION);
        set.add(SCAN_PARTITION_LATEST);
        set.add(SCAN_PARTITION_READY_MODE);
        set.add(SCAN_MODE);
        set.add(SCAN_SNAPSHOT_VERSION);
        set.add(SCAN_TIMESTAMP);
        set.add(SCAN_INCREMENTAL_MODE);
        set.add(INCREMENTAL_BETWEEN);
        set.add(INCREMENTAL_BETWEEN_TIMESTAMP);

        set.add(SINK_PARALLELISM);
        set.add(SINK_PARTITION);
        set.add(SINK_DYNAMIC_PARTITION_LIMIT);
        set.add(SINK_GROUP_PARTITION);
        set.add(SINK_FILE_CACHED_ENABLE);
        set.add(PARTITION_DEFAULT_VALUE);
        set.add(PARTITION_ASSIGNER_CLASS);
        set.add(SINK_ASYNC_COMMIT_ENABLE);
        set.add(SINK_META_CACHE_SIZE);
        set.add(SINK_META_EXPIRE_TIME);

        set.add(UPSERT_WRITE_BUCKET_NUM);
        set.add(UPSERT_WRITE_SLOT_NUM);
        set.add(UPSERT_WRITER_BUCKET_BUFFER_SIZE);
        set.add(UPSERT_WRITER_MAX_BUFFER_SIZE);
        set.add(UPSERT_WRITER_FLUSH_MAX_RETRIES);

        set.add(UPSERT_COMMIT_THREAD_NUM);
        set.add(UPSERT_COMMIT_MAX_RETRIES);
        set.add(UPSERT_COMMIT_TIMEOUT);
        set.add(UPSERT_FLUSH_CONCURRENT);
        set.add(UPSERT_NIO_THREAD_NUM);
        set.add(UPSERT_MAJOR_COMPACT_ENABLE);
        set.add(UPSERT_MAJOR_COMPACT_MIN_COMMITS);
        set.add(UPSERT_PARTIAL_COLUMN_ENABLE);
        set.add(UPSERT_PARTIAL_COLUMN_NAME);

        set.add(INSERT_STREAMING_TUNNEL_ENABLE);
        set.add(INSERT_STREAMING_TUNNEL_SLOT_NUM);
        set.add(INSERT_ARROW_WRITER_ENABLE);
        set.add(INSERT_ARROW_WRITE_BATCH_SIZE);
        set.add(INSERT_ARROW_WRITER_FLUSH_INTERVAL);
        set.add(INSERT_WRITER_BUFFER_SIZE);
        set.add(INSERT_WRITER_MAX_PENDING_BUFFERS);
        set.add(INSERT_COMMIT_THREAD_NUM);

        set.add(SINK_THREAD_NUM);
        set.add(SINK_BUCKET_CHECK_INTERVAL);
        set.add(SINK_ASYNC_FLUSH_THREAD_NUM);

        set.add(SINK_FILE_CACHE_MEMORY);
        set.add(SINK_FILE_MEMORY_SEGMENT_SIZE);
        set.add(SINK_FILE_WRITER_MAX_RETRIES);
        set.add(SINK_FILE_ROLLING_MAX_SIZE);
        set.add(SINK_FILE_FLUSH_ALWAYS);
        set.add(SINK_FILE_TMP_DIRS);
        set.add(STATE_FILE_THRESHOLD);

        set.add(RETRY_MAX_TIMES);
        set.add(RETRY_DELAY_MILLIS);
        set.add(SINK_COMPRESS_ALGO);

        set.add(SINK_STANDALONE_ENABLE);
        set.add(SINK_COORDINATOR_ENABLE);

        set.add(SCAN_DISK_SPILL_ENABLED);
        set.add(SCAN_DISK_SPILL_MEMORY_CAPACITY);
        set.add(SCAN_DISK_SPILL_MAX_BYTES);
        set.add(SINK_FLUSH_MAX_INTERVAL);
        set.add(READ_TIMEOUT);
        set.add(SINK_DYNAMIC_SHUFFLE_ENABLE);
        set.add(SINK_SHUFFLE_BUCKET_ONLY_ENABLE);
        return set;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        context = normalizeContext(this, context);
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();
        helper.validate();
        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        validateConfigOptions(config);

        WriteOperationType type = checkWriteOptions(config, resolvedSchema);
        TableSchema schema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        OdpsConf odpsConf = Preconditions.checkNotNull(getOdpsConf(config),
                "Odps conf cannot be null");
        return new OdpsDynamicTableSink(
                config,
                odpsConf,
                OdpsTablePath.fromTablePath(odpsConf.getProject(),
                        config.get(TABLE_PATH), config.get(ODPS_NAMESPACE_SCHEMA_ENABLE)),
                schema,
                context.getCatalogTable().getPartitionKeys(),
                type,
                config.get(OdpsOptions.SINK_PARALLELISM));
    }

    private static DynamicTableFactory.Context normalizeContext(DynamicTableFactory factory, DynamicTableFactory.Context context) {
        Map<String, String> catalogOptions = context.getCatalogTable().getOptions();
        Map<String, String> convertedOptions = normalizeOptionCaseAsFactory(factory, catalogOptions);
        return new FactoryUtil.DefaultDynamicTableContext(context.getObjectIdentifier(), context.getCatalogTable().copy(convertedOptions), context.getEnrichmentOptions(), context.getConfiguration(), context.getClassLoader(), context.isTemporary());
    }

    private static Map<String, String> normalizeOptionCaseAsFactory(Factory factory, Map<String, String> options) {
        Map<String, String> normalizedOptions = new HashMap();
        Map<String, String> requiredOptionKeysLowerCaseToOriginal = (Map) factory.requiredOptions().stream().collect(Collectors.toMap((option) -> {
            return option.key().toLowerCase();
        }, ConfigOption::key));
        Map<String, String> optionalOptionKeysLowerCaseToOriginal = (Map) factory.optionalOptions().stream().collect(Collectors.toMap((option) -> {
            return option.key().toLowerCase();
        }, ConfigOption::key));
        Iterator var5 = options.entrySet().iterator();

        while (var5.hasNext()) {
            Map.Entry<String, String> entry = (Map.Entry) var5.next();
            String catalogOptionKey = (String) entry.getKey();
            String catalogOptionValue = (String) entry.getValue();
            normalizedOptions.put(requiredOptionKeysLowerCaseToOriginal.containsKey(catalogOptionKey.toLowerCase()) ? (String) requiredOptionKeysLowerCaseToOriginal.get(catalogOptionKey.toLowerCase()) : (String) optionalOptionKeysLowerCaseToOriginal.getOrDefault(catalogOptionKey.toLowerCase(), catalogOptionKey), catalogOptionValue);
        }

        return normalizedOptions;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        context = normalizeContext(this, context);
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();
        helper.validate();
        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        validateConfigOptions(config);

        WriteOperationType type = checkWriteOptions(config, resolvedSchema);
        TableSchema schema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        OdpsConf odpsConf = Preconditions.checkNotNull(getOdpsConf(config),
                "Odps conf cannot be null");
        OdpsTablePath path = OdpsTablePath.fromTablePath(odpsConf.getProject(),
                config.get(TABLE_PATH),
                config.get(ODPS_NAMESPACE_SCHEMA_ENABLE));

        TableIdentifier identifier = TableIdentifier.of(path.getProjectName(),
                path.getSchemaName() == null ? "default" : path.getSchemaName(),
                path.getTableName());

        boolean isStreamingMode =
                context.getConfiguration().get(ExecutionOptions.RUNTIME_MODE)
                        == RuntimeExecutionMode.STREAMING;

        return new OdpsDynamicTableSource(
                config,
                odpsConf,
                OdpsLookupOptions.builder().build(),
                identifier,
                schema,
                context.getCatalogTable().getPartitionKeys(),
                resolvedSchema,
                isStreamingMode);
    }

    private void validateConfigOptions(ReadableConfig config) {
        checkAllOrNone(
                config,
                new ConfigOption[]{
                        PROJECT_NAME,
                        END_POINT
                });
    }

    /**
     * Validate the table type.
     */
    private WriteOperationType checkWriteOptions(ReadableConfig conf, ResolvedSchema schema) {
        String operation = conf.get(SINK_OPERATION);

        WriteOperationType type;
        try {
            type = WriteOperationType.fromValue(operation);
        } catch (IllegalArgumentException e) {
            throw new FlinkOdpsException("Invalid sink operation type: " + operation);
        }

        if (type.equals(WriteOperationType.UPSERT)) {
            if (!schema.getPrimaryKey().isPresent()) {
                throw new FlinkOdpsException("Primary key does not exist in the table schema.");
            }
        }
        return type;
    }

    private void checkAllOrNone(ReadableConfig config, ConfigOption<?>[] configOptions) {
        int presentCount = 0;
        for (ConfigOption configOption : configOptions) {
            if (config.getOptional(configOption).isPresent()) {
                presentCount++;
            }
        }
        String[] propertyNames =
                Arrays.stream(configOptions).map(ConfigOption::key).toArray(String[]::new);
        Preconditions.checkArgument(
                configOptions.length == presentCount || presentCount == 0,
                "Either all or none of the following options should be provided:\n"
                        + String.join("\n", propertyNames));
    }

    private OdpsConf getOdpsConf(ReadableConfig readableConfig) {
        final Optional<String> accessId = readableConfig.getOptional(ACCESS_ID);
        final Optional<String> credentialsProvider = readableConfig.getOptional(CREDENTIALS_PROVIDER);

        if (accessId.isPresent() || credentialsProvider.isPresent()) {
            OdpsConf conf = new OdpsConf(readableConfig.get(ACCESS_ID),
                                         readableConfig.get(ACCESS_KEY),
                                         readableConfig.get(END_POINT),
                                         readableConfig.get(PROJECT_NAME),
                                         readableConfig.get(TUNNEL_END_POINT));
            conf.setCredentialsProviderClass(readableConfig.getOptional(CREDENTIALS_PROVIDER).orElse(null));
            conf.setCredentialsProviderParams(readableConfig.getOptional(CREDENTIALS_PROVIDER_PARAM).orElse(null));
            conf.setRegionId(readableConfig.getOptional(REGION_ID).orElse(null));
            conf.setSignatureCorporation(readableConfig.get(SIGNATURE_CORPORATION));
            return conf;
        } else {
            // use odps catalog
            return this.odpsConf;
        }
    }
}
