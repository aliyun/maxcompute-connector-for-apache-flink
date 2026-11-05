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

package org.apache.flink.odps.sink.cache;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class PartitionRecordReader {
    private static final Logger LOG = LoggerFactory.getLogger(PartitionRecordReader.class);

    private RowData currentRecord;

    private boolean isFinished;
    private boolean loadNextBuffer;
    private final RecordDeserializer<RowDataDelegate> recordDeserializer;
    private final ResultSubpartitionView subpartitionView;
    private final RowDataDelegate rowDataDelegate;

    private final Counter numBytesIn;
    private final Counter numBuffersIn;

    public PartitionRecordReader(SubPartition subPartition,
                                 String[] tmpDirectories,
                                 TypeSerializer<RowData> serializer) throws IOException {
        this.recordDeserializer = new SpillingAdaptiveSpanningRecordDeserializer<>(tmpDirectories);
        this.subpartitionView = subPartition.createReadView(org.apache.flink.odps.shim.FlinkSubpartitionView.noOpListener());
        this.loadNextBuffer = true;
        this.isFinished = false;
        this.rowDataDelegate = new RowDataDelegate(serializer);
        this.numBytesIn = new SimpleCounter();
        this.numBuffersIn = new SimpleCounter();
    }

    public boolean hasNext() throws IOException {
        if (currentRecord != null) {
            return true;
        } else {
            if (getNextRecord()) {
                return true;
            } else {
                return false;
            }
        }
    }

    public RowData next() throws IOException {
        if (hasNext()) {
            RowData tmp = currentRecord;
            currentRecord = null;
            return tmp;
        } else {
            return null;
        }
    }

    protected boolean getNextRecord() throws IOException {
        if (isFinished) {
            return false;
        }

        while (true) {
            if (loadNextBuffer) {
                ResultSubpartition.BufferAndBacklog next = subpartitionView.getNextBuffer();
                if (next == null) {
                    isFinished = true;
                    return false;
                }

                Buffer buffer = next.buffer();
                numBytesIn.inc(buffer.getSize());
                numBuffersIn.inc();

                recordDeserializer.setNextBuffer(buffer);
            }

            RecordDeserializer.DeserializationResult result = recordDeserializer.getNextRecord(rowDataDelegate);

            loadNextBuffer = result.isBufferConsumed();

            if (result.isFullRecord()) {
                currentRecord = rowDataDelegate.getRowData();
                return true;
            }
        }
    }

    public void clearBuffers() {
        recordDeserializer.clear();
    }

    public void releaseAllResources() throws IOException {
        subpartitionView.releaseAllResources();
    }
}
