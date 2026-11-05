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

package org.apache.flink.odps.source.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Set;

public class OdpsRecords implements RecordsWithSplitIds<RowData> {

    @Nullable
    private String splitId;

    @Nullable
    private RecordIterator<RowData> recordsForSplitCurrent;

    @Nullable
    private final RecordIterator<RowData> recordsForSplit;

    private final Set<String> finishedSplits;

    private OdpsRecords(
            @Nullable String splitId,
            @Nullable RecordIterator<RowData> recordsForSplit,
            Set<String> finishedSplits) {
        this.splitId = splitId;
        this.recordsForSplit = recordsForSplit;
        this.finishedSplits = finishedSplits;
    }

    @Nullable
    @Override
    public String nextSplit() {
        // move the split one (from current value to null)
        final String nextSplit = this.splitId;
        this.splitId = null;

        // move the iterator, from null to value (if first move) or to null (if second move)
        this.recordsForSplitCurrent = nextSplit != null ? this.recordsForSplit : null;

        return nextSplit;
    }

    @Nullable
    @Override
    public RowData nextRecordFromSplit() {
        final RecordIterator<RowData> recordsForSplit = this.recordsForSplitCurrent;
        if (recordsForSplit != null) {
            return recordsForSplit.next();
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public void recycle() {
        if (recordsForSplit != null) {
            recordsForSplit.releaseBatch();
        }
    }

    @Override
    public Set<String> finishedSplits() {
        return finishedSplits;
    }

    // ------------------------------------------------------------------------

    public static OdpsRecords forRecords(
            final String splitId, final RecordIterator<RowData> recordsForSplit) {
        return new OdpsRecords(splitId, recordsForSplit, Collections.emptySet());
    }

    public static OdpsRecords finishedSplit(String splitId) {
        return new OdpsRecords(null, null, Collections.singleton(splitId));
    }

}
