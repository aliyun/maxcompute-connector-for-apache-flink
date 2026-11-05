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
import com.aliyun.odps.utils.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.sink.table.TableUpsertSessionImpl;
import org.apache.flink.odps.sink.table.TableUpsertWriterOptions;
import org.apache.flink.odps.sink.table.TableUpsertWriterOptions.Builder;
import org.apache.flink.odps.sink.table.UpsertWriter;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.odps.table.OdpsOptions.*;

public class OdpsUpsertWriter extends OdpsTableWriter {

    private static final Logger LOG = LoggerFactory.getLogger(OdpsUpsertWriter.class);

    private TableUpsertSessionImpl upsertSession;

    private UpsertWriter<ArrayRecord> upsertWriter;

    private long totalRecord;

    private TableUpsertWriterOptions writerOptions;

    private boolean finished;

    private boolean started;

    private final TableIdentifier identifier;

    private final String sessionId;

    public OdpsUpsertWriter(Configuration config,
                            OdpsConf odpsConf,
                            TableIdentifier identifier,
                            String partition,
                            DataSchema schema,
                            String sessionId,
                            RowType rowType) throws IOException {
        super(config, odpsConf, identifier, partition, schema, rowType);
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
        this.identifier = identifier;
        this.sessionId = sessionId;
        this.finished = false;
        this.totalRecord = 0L;
    }

    private void ensureStarted() throws IOException {
        if (!started) {
            started = true;
            upsertSession = OdpsUtils.getOrCreateUpsertSession(identifier, partition, sessionId, settings);
            upsertWriter = upsertSession.createUpsertWriter(writerOptions);
        }
    }

    @Override
    public void addElement(RowData element) throws IOException {
        if (finished) {
            throw new IOException("Add element failed: upsert writer has been finished or aborted!");
        }

        ensureStarted();

        ArrayRecord record = upsertWriter.newElement();
        converter.convert(element, record);
        if (element.getRowKind().equals(RowKind.INSERT)
                || element.getRowKind().equals(RowKind.UPDATE_AFTER)) {
            upsertWriter.upsert(record);
        } else if (element.getRowKind().equals(RowKind.DELETE)
                || element.getRowKind().equals(RowKind.UPDATE_BEFORE)) {
            upsertWriter.delete(record);
        } else {
            LOG.error("Ignore element:" + element.getRowKind());
        }
    }

    @Override
    public void flush() throws IOException {
        if (finished) {
            throw new IOException("Flush failed: upsert writer has been finished or aborted!");
        }

        ensureStarted();

        if (upsertWriter != null) {
            upsertWriter.flush();
        }
    }

    @Override
    public CommitInfo finish(long checkpointId) throws IOException {
        if (finished) {
            throw new IOException("Finish failed: upsert writer has been finished or aborted!");
        }

        try {
            if (upsertWriter != null) {
                upsertWriter.close();
                totalRecord += upsertWriter.currentMetricsValues()
                            .counter("recordCount").get().getCount();
                upsertWriter = null;
            }
        } finally {
            if (upsertSession != null) {
                upsertSession.close();
            }
        }

        PartitionCommitInfo commitInfo = new PartitionCommitInfo();
        commitInfo.setPartitionSpec(partition);
        commitInfo.setSessionId(sessionId);
        commitInfo.setTotalRecords(totalRecord);
        commitInfo.setCheckpointId(checkpointId);

        finished = true;
        return commitInfo;
    }

    @Override
    public void reset() {
        try {
            if (upsertWriter != null) {
                upsertWriter.reset();
            }
        } catch (IOException e) {
            LOG.error("Reset upsert writer error", e);
        }
    }

    @Override
    public void abort() {
        if (!finished) {
            try {
                if (upsertWriter != null) {
                    upsertWriter.reset();
                    upsertWriter = null;
                }
            } catch (IOException e) {
                LOG.error("Abort upsert writer error", e);
            } finally {
                if (upsertSession != null) {
                    upsertSession.close();
                    upsertSession = null;
                }
            }
            finished = true;
        }
    }
}
