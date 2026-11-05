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

package org.apache.flink.odps.sink.upsert;

import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.utils.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.FlinkOdpsException;
import org.apache.flink.odps.sink.insert.InsertFunction;
import org.apache.flink.odps.sink.common.WriteOperationType;
import org.apache.flink.odps.sink.partition.PartitionAssigner;
import org.apache.flink.odps.sink.common.OdpsWriteOptions;
import org.apache.flink.odps.sink.table.TableUpsertSessionImpl;
import org.apache.flink.odps.sink.table.TableUpsertWriterOptions;
import org.apache.flink.odps.sink.table.TableUpsertWriterOptions.Builder;
import org.apache.flink.odps.sink.table.UpsertWriter;
import org.apache.flink.odps.sink.utils.DataBucket;
import org.apache.flink.odps.sink.utils.DataItem;
import org.apache.flink.odps.sink.utils.WriterStatus;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.flink.odps.table.OdpsOptions.*;

public class UpsertFunction extends InsertFunction {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(UpsertFunction.class);

    protected transient Map<String, TableUpsertSessionImpl> odpsUpsertSessionMap;
    protected transient Map<String, UpsertWriter<ArrayRecord>> odpsUpsertWriterMap;

    private transient TableUpsertWriterOptions writerOptions;

    public UpsertFunction(
            Configuration config,
            OdpsConf odpsConf,
            TableIdentifier identifier,
            String partition,
            DataSchema schema,
            boolean isDynamicPartition,
            boolean isGroupPartition,
            boolean supportsGrouping,
            OdpsWriteOptions writeOptions,
            PartitionAssigner<RowData> partitionAssigner,
            WriteOperationType writeOperationType, RowType rowType) {
        super(config, odpsConf, identifier, partition, schema,
                isDynamicPartition, isGroupPartition, supportsGrouping,
                writeOptions, partitionAssigner, writeOperationType, rowType);
    }

    @Override
    public void open(Configuration parameters) throws IOException {
        super.open(parameters);

        this.odpsUpsertWriterMap = new HashMap<>();
        this.odpsUpsertSessionMap = new HashMap<>();

        Builder upsertWriterOptionBuilder = TableUpsertWriterOptions.builder()
            .withMaxRetries(config.getInteger(UPSERT_WRITER_FLUSH_MAX_RETRIES))
            .withMaxBufferSize(config.get(UPSERT_WRITER_MAX_BUFFER_SIZE).getBytes())
            .withSlotBufferSize(config.get(UPSERT_WRITER_BUCKET_BUFFER_SIZE).getBytes())
            .withCompressAlgorithm(config.getString(SINK_COMPRESS_ALGO));
        if (config.getBoolean(UPSERT_PARTIAL_COLUMN_ENABLE)) {
            upsertWriterOptionBuilder.enablePartialWrite(true);
            String partialColumnName = config.getString(UPSERT_PARTIAL_COLUMN_NAME);
            if (StringUtils.isNotBlank(partialColumnName)) {
                List<String> partialColumnList = Arrays.stream(partialColumnName.split(",")).map(String::trim).collect(
                    Collectors.toList());
                upsertWriterOptionBuilder.withPartialWriteColumns(partialColumnList);
            }
        }
        this.writerOptions = upsertWriterOptionBuilder.build();
    }

    @Override
    protected void bootstrapForGroupPartition(String currentPartition) throws IOException {
        OdpsUtils.createPartitionIfNeeded(getTableMetaProvider(), tableIdentifier, currentPartition);
        TableUpsertSessionImpl writeSession =
                OdpsUtils.getOrCreateUpsertSession(tableIdentifier, currentPartition, null, settings);
        odpsUpsertSessionMap.put(currentPartition, writeSession);
        sessionRequest.put(currentPartition, writeSession.getId());
    }

    @Override
    protected void flushBucketData(String currentPartition, DataBucket bucket) throws IOException {
        if (!bucket.isEmpty()) {
            try {
                if (!odpsUpsertWriterMap.containsKey(currentPartition)) {
                    initUpsertWriter(currentPartition, sessionRequest.get(currentPartition));
                }

                UpsertWriter<ArrayRecord> upsertWriter = odpsUpsertWriterMap.get(currentPartition);
                ArrayRecord upsertRecord = upsertWriter.newElement();

                List<DataItem> items = bucket.getRecords();
                for (DataItem dataItem : items) {
                    ArrayRecord arrayRecord = dataItem.getData();
                    for (int i = 0; i < arrayRecord.getColumnCount(); i++) {
                        upsertRecord.set(i, arrayRecord.get(i));
                    }
                    doWriteRecord(currentPartition, upsertRecord, dataItem.getRowKind());
                }
            } catch (IOException e) {
                throw new FlinkOdpsException("Flush data error: ", e);
            }
            bucket.reset();
        }
    }

    @Override
    protected void commitWriter() throws IOException {
        for (Map.Entry<String, UpsertWriter<ArrayRecord>> writerEntry :
                odpsUpsertWriterMap.entrySet()) {
            String part = writerEntry.getKey();

            LOG.info("Close Odps writer for partition {}", part);

            UpsertWriter<ArrayRecord> upsertWriter = writerEntry.getValue();

            TableUpsertSessionImpl session = odpsUpsertSessionMap.get(part);
            String sessionId = session.getId();
            try {
                upsertWriter.close();
            } finally {
                session.close();
            }

            WriterStatus writerStatus = new WriterStatus();
            writerStatus.setSessionId(sessionId);
            writerStatus.setPartitionSpec(part);
            writerStatus.setTotalRecords(
                    upsertWriter.currentMetricsValues()
                            .counter("recordCount").get().getCount());
            statuses.add(writerStatus);
        }
    }

    @Override
    protected void reset() {
        super.reset();

        odpsUpsertWriterMap.forEach((part, writer) -> {
            try {
                writer.reset();
            } catch (IOException ignored) {
            }
        });
        odpsUpsertWriterMap.clear();

        odpsUpsertSessionMap.forEach((part, session) -> {
            session.close();
        });
        odpsUpsertSessionMap.clear();
    }

    @Override
    protected void validDateRowData(RowData in) throws Exception {
        // Do nothing
    }

    @Override
    protected void writeData(String currentPartition, RowData in) throws IOException {
        if (!odpsUpsertWriterMap.containsKey(currentPartition)) {
            initUpsertWriter(currentPartition, sessionRequest.get(currentPartition));
        }
        UpsertWriter<ArrayRecord> upsertWriter = odpsUpsertWriterMap.get(currentPartition);
        ArrayRecord record = upsertWriter.newElement();
        converter.convert(in, record);
        doWriteRecord(currentPartition, record, in.getRowKind());
    }

    private void doWriteRecord(String partitionSpec, ArrayRecord record, RowKind rowKind)
            throws IOException {
        switch (rowKind) {
            case INSERT:
            case UPDATE_AFTER:
                odpsUpsertWriterMap.get(partitionSpec).upsert(record);
                break;
            case DELETE:
            case UPDATE_BEFORE:
                odpsUpsertWriterMap.get(partitionSpec).delete(record);
                break;
            default:
                throw new UnsupportedOperationException();
        }
    }

    private UpsertWriter<ArrayRecord> initUpsertWriter(String targetPartition,
                                                       String sessionId) throws IOException {
        TableUpsertSessionImpl upsertSession;

        if (odpsUpsertSessionMap.containsKey(targetPartition)) {
            upsertSession = odpsUpsertSessionMap.get(targetPartition);
        } else {
            upsertSession = OdpsUtils.getOrCreateUpsertSession(tableIdentifier, targetPartition, sessionId, settings);
            odpsUpsertSessionMap.put(targetPartition, upsertSession);
        }

        UpsertWriter<ArrayRecord> writer = upsertSession.createUpsertWriter(writerOptions);
        odpsUpsertWriterMap.put(targetPartition, writer);

        return writer;
    }
}