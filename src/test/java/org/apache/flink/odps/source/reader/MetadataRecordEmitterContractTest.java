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
package org.apache.flink.odps.source.reader;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.odps.source.split.OdpsSourceSplitState;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/** Metadata projection must retain changelog semantics and resume only accepted records. */
public class MetadataRecordEmitterContractTest {
    @Test
    public void preservesRequestedMetadataOrderAndRestoredOffsets() {
        MetadataRecordEmitter emitter = new MetadataRecordEmitter(Arrays.asList("bucket_time", "bucket_id"));
        OdpsSourceSplitState state = new OdpsSourceSplitState("restored", 0, null, null, 7);
        RecordingOutput output = new RecordingOutput();
        GenericRowData first = GenericRowData.of(41L);
        first.setRowKind(RowKind.DELETE);
        emitter.emitRecord(first, output, state);
        emitter.emitRecord(GenericRowData.of(42L), output, state);

        assertEquals(Arrays.asList(41L, 42L), output.values);
        assertEquals(Arrays.asList(RowKind.DELETE, RowKind.INSERT), output.kinds);
        assertEquals(Arrays.asList(-1L, -1L), output.times);
        assertEquals(Arrays.asList(-1, -1), output.buckets);
        assertEquals(9L, state.toSourceSplit().getRecordsToSkip());
    }

    @Test
    public void failedDeliveryDoesNotAdvanceTheCheckpointOffset() {
        MetadataRecordEmitter emitter = new MetadataRecordEmitter(Arrays.asList("bucket_id"));
        OdpsSourceSplitState state = new OdpsSourceSplitState("restored", 0, null, null, 7);
        SourceOutput<RowData> rejecting = new RecordingOutput() {
            @Override
            public void collect(RowData record) {
                throw new IllegalStateException("downstream rejected the record");
            }
        };
        assertThrows(IllegalStateException.class, () -> emitter.emitRecord(GenericRowData.of(42L), rejecting, state));
        assertEquals(7L, state.toSourceSplit().getRecordsToSkip());
    }

    private static class RecordingOutput implements SourceOutput<RowData> {
        final List<Long> values = new ArrayList<>();
        final List<RowKind> kinds = new ArrayList<>();
        final List<Long> times = new ArrayList<>();
        final List<Integer> buckets = new ArrayList<>();

        @Override
        public void collect(RowData record) {
            assertEquals(3, record.getArity());
            values.add(record.getLong(0));
            kinds.add(record.getRowKind());
            times.add(record.getTimestamp(1, 3).getMillisecond());
            buckets.add(record.getInt(2));
        }

        @Override
        public void collect(RowData record, long timestamp) {
            collect(record);
        }

        @Override
        public void emitWatermark(Watermark watermark) {}

        @Override
        public void markIdle() {}

        @Override
        public void markActive() {}
    }
}
