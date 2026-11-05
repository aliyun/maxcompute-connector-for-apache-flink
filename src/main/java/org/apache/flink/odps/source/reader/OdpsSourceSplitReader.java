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

package org.apache.flink.odps.source.reader;

import com.aliyun.odps.table.configuration.CompressionCodec;
import com.aliyun.odps.table.configuration.DiskSpillBufferOptions;
import com.aliyun.odps.table.configuration.ReaderOptions;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.read.TableBatchReadSession;
import com.aliyun.odps.table.read.TableIncrementalReadSession;
import com.aliyun.odps.table.read.split.InputSplit;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.odps.sink.table.TableUtils;
import org.apache.flink.odps.source.split.OdpsSourceSplit;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.ZoneId;
import java.util.*;

/** A reader for polling data from the specific splits of the odps partition. */
public class OdpsSourceSplitReader implements SplitReader<RowData, OdpsSourceSplit> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OdpsSourceSplitReader.class);

    private final OdpsConf odpsConf;
    private final Queue<OdpsSourceSplit> splits;
    private final RowType rowType;
    private final long maxFetchCount;
    private final CompressionCodec codec;

    private boolean paused;
    @Nullable private String currentSplitId;
    @Nullable private com.aliyun.odps.table.read.SplitReader<VectorSchemaRoot> currentRecordReader;
    private long currentNumRead;
    private boolean hasMetaDataColumns;
    private RecordIterator<RowData> currentFirstBatch;

    private final ZoneId zoneId;

    public OdpsSourceSplitReader(
            OdpsConf odpsConf,
            RowType rowType,
            long maxFetchCount,
            CompressionCodec codec,
            ZoneId zoneId) {
        this.odpsConf = odpsConf;
        this.codec = codec;
        this.splits = new ArrayDeque<>();
        this.maxFetchCount = maxFetchCount;

        this.rowType = rowType;
        this.zoneId = zoneId;
        this.hasMetaDataColumns = false;
    }

    @Override
    public RecordsWithSplitIds<RowData> fetch() throws IOException {
        if (paused) {
            return new EmptyRecordsWithSplitIds<>();
        }

        // check whether reader is ok
        checkSplitOrStartNext();
        Preconditions.checkArgument(
                currentRecordReader != null, "RecordReader is not initialized.");
        if (currentFirstBatch != null) {
            RecordIterator<RowData> nextBatch = currentFirstBatch;
            currentFirstBatch = null;
            return OdpsRecords.forRecords(currentSplitId, nextBatch);
        } else {
            if (currentRecordReader.hasNext()) {
                VectorSchemaRoot nextBatch = currentRecordReader.get();
                return OdpsRecords.forRecords(currentSplitId, readBatch(nextBatch, 0));
            } else {
                return finishSplit();
            }
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<OdpsSourceSplit> splitsChange) {
        if (!(splitsChange instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChange.getClass()));
        }
        LOGGER.info("handleSplitsChanges " + splitsChange.splits().get(0).splitId());
        splits.addAll(splitsChange.splits());
    }

    public void pauseOrResumeSplits(
            Collection<OdpsSourceSplit> splitsToPause,
            Collection<OdpsSourceSplit> splitsToResume) {
        for (OdpsSourceSplit split : splitsToPause) {
            if (split.splitId().equals(currentSplitId)) {
                paused = true;
                break;
            }
        }

        for (OdpsSourceSplit split : splitsToResume) {
            if (split.splitId().equals(currentSplitId)) {
                paused = false;
                break;
            }
        }
    }

    @Override
    public void wakeUp() {}

    @Override
    public void close() throws Exception {
        if (currentRecordReader != null) {
            currentRecordReader.close();
        }
    }

    private void checkSplitOrStartNext() throws IOException {
        if (currentRecordReader != null) {
            return;
        }
        final OdpsSourceSplit nextSplit = splits.poll();
        if (nextSplit == null) {
            throw new RuntimeException("Cannot fetch from another split - no split remaining");
        }
        currentSplitId = nextSplit.splitId();
        currentRecordReader = createRecordReader(nextSplit);
        currentNumRead = nextSplit.getRecordsToSkip();
        if (currentNumRead > 0) {
            seek(currentNumRead);
        }
    }

    private com.aliyun.odps.table.read.SplitReader<VectorSchemaRoot> createRecordReader(
            OdpsSourceSplit split) {
        try {
            TableBatchReadSession readSession = split.getBatchReadSession();
            InputSplit inputSplit = split.getSplit();
            EnvironmentSettings settings = TableUtils.getEnvironmentSettings(odpsConf);

            ReaderOptions.Builder builder =
                    ReaderOptions.newBuilder()
                            .withSettings(settings)
                            .withCompressionCodec(codec)
                            .withReuseBatch(false)
                            .withMaxBatchRowCount((int) maxFetchCount);

            if (odpsConf.getPropertyOrDefault("scan.disk-spill.enabled", false)) {
                int memoryCapacity = odpsConf.getPropertyOrDefault(
                        "scan.disk-spill.memory-capacity", 64);
                long maxSpillBytes = Long.parseLong(odpsConf.getPropertyOrDefault(
                        "scan.disk-spill.max-bytes",
                        String.valueOf(10L * 1024 * 1024 * 1024)));
                DiskSpillBufferOptions spillOptions = DiskSpillBufferOptions.newBuilder()
                        .withMemoryBufferCapacity(memoryCapacity)
                        .withMaxSpillBytes(maxSpillBytes)
                        .build();
                builder.withDiskSpillBuffer(spillOptions);
            }

            ReaderOptions readerOptions = builder.build();
            hasMetaDataColumns = readSession instanceof TableIncrementalReadSession;
            return readSession.createArrowReader(inputSplit, readerOptions);
        } catch (IOException e) {
            LOGGER.warn("fail to open record reader of table", e);
            throw new RuntimeException("fail to open record reader of table", e);
        }
    }

    private void seek(long toSkip) throws IOException {
        // TODO: storage api support skip rows
        while (true) {
            if (currentRecordReader != null && currentRecordReader.hasNext()) {
                VectorSchemaRoot nextBatch = currentRecordReader.get();
                if (toSkip > 0) {
                    if (toSkip >= nextBatch.getRowCount()) {
                        toSkip -= nextBatch.getRowCount();
                    } else {
                        currentFirstBatch = readBatch(nextBatch, (int)toSkip);
                        return;
                    }
                }
                nextBatch.close();
                if (toSkip == 0) {
                    return;
                }
            } else {
                if (toSkip > 0) {
                    throw new RuntimeException(
                            String.format(
                                    "skip(%s) more than the number of remaining elements.", toSkip));
                }
            }
        }
    }

    private OdpsRecords finishSplit() throws IOException {
        if (currentRecordReader != null) {
            currentRecordReader.close();
            currentRecordReader = null;
        }

        final OdpsRecords finishRecords = OdpsRecords.finishedSplit(currentSplitId);
        currentSplitId = null;
        return finishRecords;
    }

    public RecordIterator<RowData> readBatch(VectorSchemaRoot root, int startPos) {
        return new ColumnarRowIterator(root, rowType, zoneId, hasMetaDataColumns, startPos);
    }

    /**
     * An empty implementation of {@link RecordsWithSplitIds}. It is used to indicate that the
     * {@link OdpsSourceSplitReader} is paused or wakeup.
     */
    private static class EmptyRecordsWithSplitIds<T> implements RecordsWithSplitIds<T> {

        @Nullable
        @Override
        public String nextSplit() {
            return null;
        }

        @Nullable
        @Override
        public T nextRecordFromSplit() {
            return null;
        }

        @Override
        public Set<String> finishedSplits() {
            return Collections.emptySet();
        }
    }
}
