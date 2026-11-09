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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.WriterOptions;
import com.aliyun.odps.table.write.BatchWriter;
import com.aliyun.odps.table.write.TableBatchWriteSession;
import com.aliyun.odps.table.write.WriterAttemptId;
import com.aliyun.odps.table.write.WriterCommitMessage;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.operators.ProcessingTimeService.ProcessingTimeCallback;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.odps.sink.common.OdpsRecordConverter;
import org.apache.flink.odps.sink.common.OdpsWriteFunction;
import org.apache.flink.odps.sink.common.OdpsWriteOptions;
import org.apache.flink.odps.sink.common.WriteOperationType;
import org.apache.flink.odps.sink.event.SinkTaskEvent;
import org.apache.flink.odps.sink.event.TaskAckEvent;
import org.apache.flink.odps.sink.partition.PartitionAssigner;
import org.apache.flink.odps.sink.table.TunnelWriterOptions;
import org.apache.flink.odps.sink.utils.DataBucket;
import org.apache.flink.odps.sink.utils.DataItem;
import org.apache.flink.odps.sink.utils.WriterStatus;
import org.apache.flink.odps.util.ArrowUtils;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.odps.util.RowDataToOdpsConverters;
import org.apache.flink.odps.vectorized.ArrowWriter;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.odps.table.OdpsOptions.INSERT_ARROW_WRITER_ENABLE;
import static org.apache.flink.odps.table.OdpsOptions.INSERT_ARROW_WRITER_FLUSH_INTERVAL;
import static org.apache.flink.odps.table.OdpsOptions.INSERT_ARROW_WRITE_BATCH_SIZE;
import static org.apache.flink.odps.table.OdpsOptions.INSERT_STREAMING_TUNNEL_ENABLE;
import static org.apache.flink.odps.table.OdpsOptions.INSERT_STREAMING_TUNNEL_SLOT_NUM;
import static org.apache.flink.odps.table.OdpsOptions.INSERT_WRITER_BUFFER_SIZE;

public class InsertFunction extends OdpsWriteFunction<RowData> implements ProcessingTimeCallback {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(InsertFunction.class);

    protected final boolean isGroupPartition;
    protected final boolean supportsGrouping;
    protected transient OdpsRecordConverter converter;
    protected transient Map<String, DataBucket> buckets;

    protected transient Map<String, TableBatchWriteSession> odpsSessionMap;
    protected transient boolean useArrowWriter;
    protected transient boolean useStreamingTunnel;
    protected transient long bufferSize;
    protected transient long batchSize;
    protected transient long writerCheckInterval;
    protected transient int streamingTunnelSlotNum;

    protected transient Map<String, BatchWriter<ArrayRecord>> odpsRecordWriterMap;
    protected transient Map<String, ArrowWriter<RowData>> arrowWriterMap;
    protected transient Map<String, BatchWriter<VectorSchemaRoot>> odpsArrowWriterMap;
    protected transient Map<String, Long> odpsArrowWriterUpdateTime;
    protected transient Map<String, Long> odpsArrowWriterBlockNumber;

    // partition -> session id
    protected transient ConcurrentHashMap<String, String> sessionRequest;
    protected transient List<WriterStatus> statuses;

    protected transient volatile boolean commitConfirming;

    protected transient TableSchema odpsTableSchema;

    public InsertFunction(
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
            WriteOperationType writeOperationType,
            RowType rowType) {
        super(config, odpsConf, identifier, partition, schema,
                isDynamicPartition, writeOptions, partitionAssigner, writeOperationType, rowType);
        this.isGroupPartition = isGroupPartition;
        this.supportsGrouping = supportsGrouping;
    }

    private transient RowDataSerializer bufferedRowSerializer;

    @Override
    public void open(Configuration parameters) throws IOException {
        super.open(parameters);

        this.converter = RowDataToOdpsConverters.createRecordConverter(rowType, dataSchema);
        this.bufferedRowSerializer = new RowDataSerializer(rowType);

        this.buckets = new HashMap<>();
        this.odpsSessionMap = new HashMap<>();
        this.odpsRecordWriterMap = new HashMap<>();
        this.arrowWriterMap = new HashMap<>();
        this.odpsArrowWriterMap = new HashMap<>();
        this.odpsArrowWriterUpdateTime = new HashMap<>();
        this.odpsArrowWriterBlockNumber = new HashMap<>();

        this.sessionRequest = new ConcurrentHashMap<>();
        this.statuses = new ArrayList<>();
        this.commitConfirming = false;

        this.useStreamingTunnel = config.getBoolean(INSERT_STREAMING_TUNNEL_ENABLE);
        
        // Skip Arrow writer initialization in streaming tunnel mode for better performance
        if (useStreamingTunnel) {
            this.useArrowWriter = false;
        } else {
            this.batchSize = config.getLong(INSERT_ARROW_WRITE_BATCH_SIZE);
            this.useArrowWriter = config.getBoolean(INSERT_ARROW_WRITER_ENABLE);
            this.writerCheckInterval = config.getLong(INSERT_ARROW_WRITER_FLUSH_INTERVAL);
            
            if (useArrowWriter) {
                long currentProcessingTime = processingTimeService.getCurrentProcessingTime();
                processingTimeService.registerTimer(currentProcessingTime + writerCheckInterval,
                        this);
            }
        }
        
        this.bufferSize = config.get(INSERT_WRITER_BUFFER_SIZE).getBytes();
        this.streamingTunnelSlotNum = config.getInteger(INSERT_STREAMING_TUNNEL_SLOT_NUM);
        this.odpsTableSchema = OdpsUtils.getTableSchema(dataSchema);
    }

    @Override
    public void endInput() throws Exception {
        super.endInput();
        // Streaming tasks must return so Flink can deliver the final checkpoint
        // barrier and reopen the coordinator event gate before waiting for ACKs.
        if (streamConfig.isCheckpointingEnabled()) {
            return;
        }
        // Bounded jobs may complete without any checkpoint. Their final batch must
        // reach the coordinator and be acknowledged before the operator terminates.
        flushRemaining(true, Long.MAX_VALUE);
        while (commitConfirming) {
            executor.yield();
        }
    }

    @Override
    protected void snapshotState(long checkpoint) throws IOException {
        LOG.info("Snapshot state for checkpoint: " + checkpoint);
        acknowledgeCheckpoint(checkpoint);

        flushRemaining(inputEnded, checkpoint);
        while (commitConfirming) {
            try {
                this.executor.yield();
            } catch (InterruptedException e) {
                LOG.error("Snapshot state error: ", e);
                throw new IOException(e);
            }
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        super.notifyCheckpointAborted(checkpointId);
        reset();
        throw new IOException("Task failed for checkpoint " + checkpointId + " aborted, " +
                "this may be because too many partitions were written within a single checkpoint interval. \n" +
                "You can try the following methods: \n" +
                "1) Increasing the checkpoint interval; \n" +
                "2) Setting `sink.file-cached.enable` option to true \n" +
                "to avoid this issue.");
    }

    protected void flushRemaining(boolean endInput, long checkpointId)
            throws IOException {
        // TODO: for wait pending request & snapshot
        while (hasPendingRequest()) {
            try {
                this.executor.yield();
            } catch (InterruptedException e) {
                LOG.error("End input error: ", e);
                throw new IOException(e);
            }
        }

        if (hasBufferedData()) {
            for (Map.Entry<String, DataBucket> dataBucketEntry : buckets.entrySet()) {
                String part = dataBucketEntry.getKey();
                DataBucket bucket = dataBucketEntry.getValue();
                flushBucketData(part, bucket);
            }
        }

        commitWriter();

        if (!isDynamicPartition && statuses.isEmpty()) {
            // For static partition with empty data
            if (sessionRequest.containsKey(partition)) {
                WriterStatus writerStatus = new WriterStatus();
                writerStatus.setSessionId(sessionRequest.get(partition));
                writerStatus.setPartitionSpec(partition);
                writerStatus.setWriterMessage(null);
                writerStatus.setTotalRecords(0L);
                statuses.add(writerStatus);
            }
        }

        sendCommitEvent(endInput, checkpointId);

        reset();

        commitConfirming = true;
    }

    protected void validDateRowData(RowData in) throws Exception {
        final RowKind kind = in.getRowKind();
        if (!kind.equals(RowKind.INSERT)) {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public void processElement(RowData in, Context ctx,
                               Collector<Object> out) throws Exception {
        validDateRowData(in);

        String currentPartition = "";
        if (!isDynamicPartition) {
            currentPartition = partition;
        } else {
            currentPartition = partitionAssigner.getPartitionSpec(in, null);
        }

        if (!sessionRequest.containsKey(currentPartition)) {
            if (isGroupPartition) {
                bootstrapForGroupPartition(currentPartition);
            } else {
                sendBootstrapEvent(currentPartition);
            }
        }

        if (sessionRequest.get(currentPartition).isEmpty()) {
            // For no response
            final String targetPartitionSpec = currentPartition;
            DataBucket bucket = this.buckets.computeIfAbsent(targetPartitionSpec,
                    k -> new DataBucket(targetPartitionSpec));

            DataItem item;
            if (!useArrowWriter) {
                ArrayRecord record = new ArrayRecord(odpsTableSchema.getColumns().toArray(new Column[0]), false,
                                                     null, true);
                converter.convert(in, record);
                item = new DataItem(currentPartition, record, in.getRowKind());
            } else {
                // Source rows can reference reusable Arrow buffers which are released
                // before an asynchronous bootstrap completes. Own the buffered value.
                item = new DataItem(currentPartition, bufferedRowSerializer.copy(in));
            }
            bucket.add(item);

            if (bucket.getRecords().size() > 10000) {
                // Chained bursts must let session ACKs and checkpoint aborts run.
                executor.yield();
            }
        } else {
            if (buckets.containsKey(currentPartition)) {
                DataBucket bucket = buckets.get(currentPartition);
                flushBucketData(currentPartition, bucket);
            }

            writeData(currentPartition, in);
        }
    }

    protected void bootstrapForGroupPartition(String currentPartition) throws IOException {
        OdpsUtils.createPartitionIfNeeded(getTableMetaProvider(), tableIdentifier, currentPartition);

        TableBatchWriteSession writeSession = OdpsUtils.getOrCreateWriteSession(tableIdentifier, currentPartition, null, settings,
                                                                                useStreamingTunnel);
        odpsSessionMap.put(currentPartition, writeSession);
        sessionRequest.put(currentPartition, writeSession.getId());
    }

    protected void writeData(String currentPartition, RowData in) throws IOException {
        if (!useArrowWriter) {
            if (!odpsRecordWriterMap.containsKey(currentPartition)) {
                initRecordWriter(currentPartition, sessionRequest.get(currentPartition));
            }

            BatchWriter<ArrayRecord> upsertWriter = odpsRecordWriterMap.get(currentPartition);
            ArrayRecord record = upsertWriter.newElement();
            converter.convert(in, record);
            doWriteRecord(currentPartition, record);
        } else {
            if (!odpsArrowWriterMap.containsKey(currentPartition)) {
                initArrowWriter(currentPartition, sessionRequest.get(currentPartition));
            }

            doWriteArrowBatch(currentPartition, in);
        }
    }

    @Override
    public void close() {
        LOG.info("Close function");

        reset();
    }

    @Override
    public void onProcessingTime(long timestamp) throws Exception {
        final long currentTime = processingTimeService.getCurrentProcessingTime();
        final List<String> pendingFlushPartitions = new ArrayList<>();

        for (Map.Entry<String, Long> updateTime :
                odpsArrowWriterUpdateTime.entrySet()) {
            if (timestamp - updateTime.getValue() > writerCheckInterval) {
                pendingFlushPartitions.add(updateTime.getKey());
            }
        }

        for (String partition : pendingFlushPartitions) {
            LOG.info("Partition {} flush arrow writer {}", partition,
                    odpsArrowWriterBlockNumber.get(partition));

            ArrowWriter<RowData> writer = arrowWriterMap.get(partition);
            if (writer.getCachedCount() > 0) {
                flushArrowWriter(partition, writer);
            } else {
                flushArrowWriter(partition, writer);
                writer.close();
                closeOdpsWriter(partition, odpsArrowWriterMap.get(partition));
                odpsArrowWriterMap.remove(partition);
                odpsArrowWriterUpdateTime.remove(partition);
                arrowWriterMap.remove(partition);
            }
        }

        processingTimeService.registerTimer(currentTime + writerCheckInterval, this);
    }

    @Override
    public void handleOperatorEvent(OperatorEvent event) {
        Preconditions.checkArgument(event instanceof TaskAckEvent,
                "The write function can only handle TaskAckEvent");
        TaskAckEvent ackEvent = (TaskAckEvent) event;
        LOG.info("Handle operator event for task[{}], {}", taskID, ackEvent);
        if (ackEvent.isCommitted()) {
            commitConfirming = false;
        }
        if (!ackEvent.getSessionId().isEmpty()
                && "".equals(sessionRequest.get(ackEvent.getPartition()))) {
            sessionRequest.put(ackEvent.getPartition(),
                    ackEvent.getSessionId());
        }
    }

    protected void sendBootstrapEvent(String partition) {
        LOG.info("Send bootstrap event to coordinator, task[{}], partition: {}.", taskID, partition);
        SinkTaskEvent event = SinkTaskEvent.builder()
                .taskID(taskID)
                .bootstrap(true)
                .requiredPartition(partition)
                .build();
        eventGateway.sendEventToCoordinator(event);
        sessionRequest.put(partition, "");
    }

    protected void sendCommitEvent(boolean endInput, long checkpointId) {
        LOG.info("Send commit event to coordinator, task[{}], checkpointId[{}].", taskID, checkpointId);
        SinkTaskEvent event = SinkTaskEvent.builder()
                .taskID(taskID)
                .checkpointID(checkpointId)
                .bootstrap(false)
                .endInput(endInput)
                .writeStatus(statuses)
                .writeSuccess(true)
                .build();
        eventGateway.sendEventToCoordinator(event);
    }

    private void doWriteRecord(String partitionSpec, ArrayRecord record)
            throws IOException {
        odpsRecordWriterMap.get(partitionSpec).write(record);
    }

    private void doWriteArrowBatch(String partitionSpec, RowData rowData)
            throws IOException {
        ArrowWriter<RowData> writer = arrowWriterMap.get(partitionSpec);
        writer.write(rowData);

        if (writer.getCachedCount() > batchSize) {
            flushArrowWriter(partitionSpec, writer);
        }
    }

    private BatchWriter<ArrayRecord> initRecordWriter(String targetPartition,
                                                      String sessionId) throws IOException {
        TableBatchWriteSession writeSession;

        if (odpsSessionMap.containsKey(targetPartition)) {
            writeSession = odpsSessionMap.get(targetPartition);
        } else {
            writeSession = OdpsUtils.getOrCreateWriteSession(tableIdentifier, targetPartition, sessionId, settings,
                                                             useStreamingTunnel);
            odpsSessionMap.put(targetPartition, writeSession);
        }

        TunnelWriterOptions.Builder optionsBuilder = TunnelWriterOptions.newTunnelWriterOptionsBuilder()
                .withSettings(settings)
                .withBufferWriter(true)
                .withBufferSize(bufferSize)
                .withStreamingTunnelSlotNum(streamingTunnelSlotNum);

        if (isGroupPartition) {
            // Only one writer for group partition
            optionsBuilder.withNumberOfParallel(1);
        } else {
            optionsBuilder.withNumberOfParallel(taskParallel);
        }

        LOG.info("Init buffer writer for partition {}, session id {}, writer id {}",
                targetPartition, sessionId, taskID);

        BatchWriter<ArrayRecord> writer = writeSession.createRecordWriter(taskID,
                WriterAttemptId.of(0), optionsBuilder.build());
        odpsRecordWriterMap.put(targetPartition, writer);
        return writer;
    }

    private BatchWriter<VectorSchemaRoot> initArrowWriter(String targetPartition,
                                                          String sessionId) throws IOException {
        TableBatchWriteSession writeSession;

        if (odpsSessionMap.containsKey(targetPartition)) {
            writeSession = odpsSessionMap.get(targetPartition);
        } else {
            writeSession = OdpsUtils.getOrCreateWriteSession(tableIdentifier, targetPartition, sessionId, settings,
                                                             useStreamingTunnel);
            odpsSessionMap.put(targetPartition, writeSession);
        }

        long blockNumber = taskID;
        if (odpsArrowWriterBlockNumber.containsKey(targetPartition)) {
            blockNumber = odpsArrowWriterBlockNumber.get(targetPartition) + taskParallel;
        }

        BatchWriter<VectorSchemaRoot> writer = writeSession.createArrowWriter(blockNumber,
                WriterAttemptId.of(0), WriterOptions.newBuilder()
                        .withSettings(settings)
                        .build());
        odpsArrowWriterMap.put(targetPartition, writer);

        VectorSchemaRoot recordBatch = writer.newElement();
        ArrowWriter<RowData> arrowWriter =
                ArrowUtils.createRowDataArrowWriter(recordBatch, rowType);
        arrowWriterMap.put(targetPartition, arrowWriter);

        odpsArrowWriterUpdateTime.put(targetPartition, System.currentTimeMillis());
        odpsArrowWriterBlockNumber.put(targetPartition, blockNumber);

        LOG.info("Init arrow writer for partition {}, session id {}, writer id {}",
                targetPartition, sessionId, blockNumber);

        return writer;
    }

    protected void flushBucketData(String currentPartition, DataBucket bucket) throws IOException {
        if (!bucket.isEmpty()) {
            if (!useArrowWriter) {
                if (!odpsRecordWriterMap.containsKey(currentPartition)) {
                    initRecordWriter(currentPartition, sessionRequest.get(currentPartition));
                }
                List<DataItem> items = bucket.getRecords();
                for (DataItem dataItem : items) {
                    doWriteRecord(currentPartition, dataItem.getData());
                }

            } else {
                if (!odpsArrowWriterMap.containsKey(currentPartition)) {
                    initArrowWriter(currentPartition, sessionRequest.get(currentPartition));
                }

                List<DataItem> items = bucket.getRecords();
                for (DataItem dataItem : items) {
                    RowData data = dataItem.getRowData();
                    doWriteArrowBatch(currentPartition, data);
                }
            }
            bucket.reset();
        }
    }

    protected void commitWriter() throws IOException {
        if (!useArrowWriter) {
            for (Map.Entry<String, BatchWriter<ArrayRecord>> writerEntry :
                    odpsRecordWriterMap.entrySet()) {
                closeOdpsWriter(writerEntry.getKey(), writerEntry.getValue());
            }
        } else {

            for (Map.Entry<String, ArrowWriter<RowData>> arrowWriterEntry :
                    arrowWriterMap.entrySet()) {
                ArrowWriter<RowData> writer = arrowWriterEntry.getValue();
                flushArrowWriter(arrowWriterEntry.getKey(), writer);
                writer.close();
            }

            for (Map.Entry<String, BatchWriter<VectorSchemaRoot>> writerEntry :
                    odpsArrowWriterMap.entrySet()) {
                closeOdpsWriter(writerEntry.getKey(), writerEntry.getValue());
            }
        }
    }

    protected void reset() {
        buckets.clear();
        statuses.clear();
        sessionRequest.clear();

        odpsRecordWriterMap.forEach((key, value) -> {
            try {
                value.close();
            } catch (IOException e) {
                LOG.error("Close record writer failed: ", e);
            }
        });

        if (!useStreamingTunnel) {
            odpsSessionMap.clear();
            odpsRecordWriterMap.clear();
        }

        // Skip Arrow writer cleanup in streaming tunnel mode for better performance
        if (!useStreamingTunnel) {
            odpsArrowWriterMap.forEach((key, value) -> {
                try {
                    value.close();
                } catch (IOException e) {
                    LOG.error("Close record writer failed: ", e);
                }
            });
            odpsArrowWriterMap.clear();

            arrowWriterMap.forEach((key, value) -> value.close());
            arrowWriterMap.clear();

            odpsArrowWriterUpdateTime.clear();
            odpsArrowWriterBlockNumber.clear();
        }
    }

    private boolean hasBufferedData() {
        return this.buckets.size() > 0
                && this.buckets.values().stream().anyMatch(bucket ->
                !bucket.isEmpty());
    }

    private boolean hasPendingRequest() {
        return this.sessionRequest.size() > 0
                && this.sessionRequest.values().stream().anyMatch(String::isEmpty);
    }

    private void flushArrowWriter(String partition, ArrowWriter<RowData> writer) throws IOException {
        writer.finish();
        odpsArrowWriterMap.get(partition).write(writer.getRecordBatch());
        writer.reset();
        odpsArrowWriterUpdateTime.put(partition, System.currentTimeMillis());
    }

    private void closeOdpsWriter(String partition, BatchWriter dataWriter) throws IOException {
        LOG.info("Close Odps writer for partition {}", partition);

        TableBatchWriteSession session = odpsSessionMap.get(partition);
        String sessionId = session.getId();

        dataWriter.close();

        WriterStatus writerStatus = new WriterStatus();
        WriterCommitMessage message = dataWriter.commit();
        writerStatus.setSessionId(sessionId);
        writerStatus.setPartitionSpec(partition);
        writerStatus.setWriterMessage(message);
        writerStatus.setTotalRecords(
                dataWriter.currentMetricsValues()
                        .counter("recordCount").get().getCount());

        statuses.add(writerStatus);
    }
}