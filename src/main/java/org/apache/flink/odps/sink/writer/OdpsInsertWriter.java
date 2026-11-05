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

package org.apache.flink.odps.sink.writer;

import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.WriterOptions;
import com.aliyun.odps.table.write.*;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.sink.table.TunnelCommitMessage;
import org.apache.flink.odps.sink.table.TunnelMultiBlockCommitMessage;
import org.apache.flink.odps.sink.table.TunnelRecordWriter;
import org.apache.flink.odps.sink.table.TunnelWriterOptions;
import org.apache.flink.odps.table.OdpsOptions;
import org.apache.flink.odps.util.ArrowUtils;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.odps.vectorized.ArrowWriter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.odps.table.OdpsOptions.*;

public class OdpsInsertWriter extends OdpsTableWriter {

    private static final Logger LOG = LoggerFactory.getLogger(OdpsInsertWriter.class);

    private final TableBatchWriteSession batchWriteSession;
    private final String sessionId;
    private final boolean useArrowWriter;
    private final long bufferSize;
    private final long batchSize;
    private final int writerParallel;
    private final boolean isGroupPartition;
    private final int streamingTunnelSlotNum;

    private int currentWriterId;
    private ArrowWriter<RowData> currentArrowWriter;
    private BatchWriter<VectorSchemaRoot> currentOdpsArrowWriter;
    private BatchWriter<ArrayRecord> currentRecordWriter;
    private final List<WriterCommitMessage> commitMessages;

    private long totalRecord;
    private boolean finished;

    public OdpsInsertWriter(Configuration config,
                            OdpsConf odpsConf,
                            TableIdentifier identifier,
                            String partition,
                            DataSchema schema,
                            String sessionId,
                            int startWriterIndex,
                            int writerParallel,
                            RowType rowType) throws IOException {
        super(config, odpsConf, identifier, partition, schema, rowType);
        this.isGroupPartition = config.get(OdpsOptions.SINK_GROUP_PARTITION);
        this.bufferSize = config.get(INSERT_WRITER_BUFFER_SIZE).getBytes();
        this.batchSize = config.getLong(INSERT_ARROW_WRITE_BATCH_SIZE);
        this.useArrowWriter = config.getBoolean(INSERT_ARROW_WRITER_ENABLE);
        this.streamingTunnelSlotNum = config.getInteger(INSERT_STREAMING_TUNNEL_SLOT_NUM);
        this.batchWriteSession = OdpsUtils.getOrCreateWriteSession(identifier, partition, sessionId, settings,
                                                                   config.getBoolean(INSERT_STREAMING_TUNNEL_ENABLE));
        this.sessionId = batchWriteSession.getId();
        this.writerParallel = writerParallel;
        this.currentWriterId = startWriterIndex;
        this.finished = false;
        this.commitMessages = new ArrayList<>();
    }

    @Override
    public String sessionId() {
        return sessionId;
    }

    @Override
    public void addElement(RowData element) throws IOException {
        if (useArrowWriter) {
            doWriteArrowBatch(element);
        } else {
            doWriteRecord(element);
        }
    }

    @Override
    public void flush() throws IOException {
        if (useArrowWriter) {
            if (currentOdpsArrowWriter != null) {

                if (currentArrowWriter.getCachedCount() > 0) {
                    currentArrowWriter.finish();
                    currentOdpsArrowWriter.write(currentArrowWriter.getRecordBatch());
                    currentArrowWriter.reset();
                }
                currentOdpsArrowWriter.close();

                commitMessages.add(currentOdpsArrowWriter.commit());
                totalRecord += currentOdpsArrowWriter.currentMetricsValues()
                        .counter("recordCount").get().getCount();

                currentArrowWriter.close();

                currentOdpsArrowWriter = null;
                currentArrowWriter = null;

                currentWriterId = currentWriterId + writerParallel;
            }
        } else {
            if (currentRecordWriter != null) {
                ((TunnelRecordWriter)currentRecordWriter).flush();
            }
        }
    }

    @Override
    public CommitInfo finish(long checkpointId) throws IOException {
        if (finished) {
            throw new IOException("writer has been finished or aborted");
        }

        flush();

        WriterCommitMessage writerCommitMessage = null;
        if (!useArrowWriter) {
            if (currentRecordWriter != null) {
                currentRecordWriter.close();
                writerCommitMessage = currentRecordWriter.commit();
                totalRecord += currentRecordWriter.currentMetricsValues()
                        .counter("recordCount").get().getCount();
                currentRecordWriter = null;
            }
        } else {
            List<Long> blockIds = new ArrayList<>();
            for (WriterCommitMessage commitMessage : commitMessages) {
                if (commitMessage instanceof TunnelCommitMessage) {
                    blockIds.add(((TunnelCommitMessage)commitMessage).getBlockId());
                }
            }
            writerCommitMessage = new TunnelMultiBlockCommitMessage(sessionId, blockIds);
        }
        PartitionCommitInfo commitInfo = new PartitionCommitInfo();
        commitInfo.setPartitionSpec(partition);
        commitInfo.setSessionId(sessionId);
        commitInfo.setTotalRecords(totalRecord);
        commitInfo.setWriterMessage(writerCommitMessage);
        commitInfo.setCheckpointId(checkpointId);

        finished = true;
        return commitInfo;
    }

    @Override
    public void reset() {
        if (currentArrowWriter != null) {
            currentArrowWriter.reset();
            currentArrowWriter.close();
            currentArrowWriter = null;
        }

        this.currentOdpsArrowWriter = null;

        // TODO: Tunnel buffered writer should reset
    }

    @Override
    public void abort() {
        if (!finished) {
            reset();
            finished = true;
        }
    }

    private void doWriteArrowBatch(RowData rowData) throws IOException {
        if (currentOdpsArrowWriter == null) {
            initArrowWriter(currentWriterId);
        }
        currentArrowWriter.write(rowData);
        if (currentArrowWriter.getCachedCount() >= batchSize) {
            currentArrowWriter.finish();
            currentOdpsArrowWriter.write(currentArrowWriter.getRecordBatch());
            currentArrowWriter.reset();
        }
    }

    private void doWriteRecord(RowData rowData) throws IOException {
        if (currentRecordWriter == null) {
            initRecordWriter(currentWriterId);
        }

        ArrayRecord record = currentRecordWriter.newElement();
        converter.convert(rowData, record);
        currentRecordWriter.write(record);
    }

    private BatchWriter<ArrayRecord> initRecordWriter(int writerId) throws IOException {
        TunnelWriterOptions.Builder tunnelWriterOptions = TunnelWriterOptions.newTunnelWriterOptionsBuilder()
                .withSettings(settings)
                .withBufferWriter(true)
                .withBufferSize(bufferSize)
                .withStreamingTunnelSlotNum(streamingTunnelSlotNum);

        if (isGroupPartition) {
            // Only one writer for group partition
            tunnelWriterOptions.withNumberOfParallel(1);
        } else {
            tunnelWriterOptions.withNumberOfParallel(writerParallel);
        }

        // A grouped partition has its own session with one SDK share. Its local
        // share ID is zero, regardless of the Flink subtask that owns the partition.
        this.currentRecordWriter = batchWriteSession.createRecordWriter(isGroupPartition ? 0 : writerId,
                WriterAttemptId.of(0), tunnelWriterOptions.build());

        return currentRecordWriter;
    }

    private BatchWriter<VectorSchemaRoot> initArrowWriter(int writerId) throws IOException {
        this.currentOdpsArrowWriter = batchWriteSession.createArrowWriter(writerId,
                WriterAttemptId.of(0), WriterOptions.newBuilder()
                        .withSettings(settings)
                        .build());

        VectorSchemaRoot recordBatch = currentOdpsArrowWriter.newElement();
        this.currentArrowWriter = ArrowUtils.createRowDataArrowWriter(recordBatch, rowType);
        return currentOdpsArrowWriter;
    }
}
