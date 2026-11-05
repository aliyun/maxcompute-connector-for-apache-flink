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

package org.apache.flink.odps.source.split;

import com.aliyun.odps.table.read.TableBatchReadSession;
import com.aliyun.odps.table.read.split.InputSplit;
import com.aliyun.odps.table.read.split.InputSplitWithBucket;
import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;

public class OdpsSourceSplit implements SourceSplit, Serializable {
    private static final long serialVersionUID = 1L;
    private final String splitId;
    private final Integer splitIndex;
    private final InputSplit split;
    private final TableBatchReadSession batchReadSession;
    private final long recordsToSkip;

    public OdpsSourceSplit(String splitId,
                           Integer splitIndex,
                           InputSplit split,
                           TableBatchReadSession readSession) {
        this(splitId, splitIndex, split, readSession, 0L);
    }

    public OdpsSourceSplit(String splitId,
                           Integer splitIndex,
                           InputSplit split,
                           TableBatchReadSession readSession,
                           long recordsToSkip) {
        this.batchReadSession = readSession;
        this.splitId = splitId;
        this.split = split;
        this.splitIndex = splitIndex;
        this.recordsToSkip = recordsToSkip;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    public TableBatchReadSession getBatchReadSession() {
        return batchReadSession;
    }

    public InputSplit getSplit() {
        return split;
    }

    public Integer getSplitIndex() {
        return splitIndex;
    }

    public long getRecordsToSkip() {
        return recordsToSkip;
    }

    public int getBucketId() {
        if (split instanceof InputSplitWithBucket) {
            return ((InputSplitWithBucket) split).getBucketId();
        }
        return -1;
    }
}
