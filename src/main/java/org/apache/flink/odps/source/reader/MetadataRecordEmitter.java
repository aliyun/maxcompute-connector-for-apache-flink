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

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.odps.source.split.OdpsSourceSplitState;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.utils.JoinedRowData;

import java.util.List;

public class MetadataRecordEmitter
        implements RecordEmitter<RowData, RowData, OdpsSourceSplitState> {

    private final List<String> metadataKeys;
    private final JoinedRowData joinedRow = new JoinedRowData();

    public MetadataRecordEmitter(List<String> metadataKeys) {
        this.metadataKeys = metadataKeys;
    }

    @Override
    public void emitRecord(
            RowData element,
            SourceOutput<RowData> output,
            OdpsSourceSplitState splitState) {
        GenericRowData metaRow = new GenericRowData(metadataKeys.size());
        int bucketId = splitState.getBucketId();
        for (int i = 0; i < metadataKeys.size(); i++) {
            switch (metadataKeys.get(i)) {
                case "bucket_id":
                    metaRow.setField(i, bucketId);
                    break;
                case "bucket_time":
                    metaRow.setField(i, TimestampData.fromEpochMillis(bucketId));
                    break;
            }
        }
        joinedRow.replace(element, metaRow);
        joinedRow.setRowKind(element.getRowKind());
        output.collect(joinedRow);
        splitState.updateRecordsToSkip();
    }
}
