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

package org.apache.flink.odps.source.bucket;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.odps.source.split.OdpsSourceSplit;
import org.apache.flink.odps.source.split.OdpsSourceSplitState;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Supplier;

/**
 * SourceReader that outputs {@link BucketTaggedRow} instead of plain RowData.
 * Reuses the same SplitReader as the standard ODPS source.
 */
public class BucketAwareSourceReader
        extends SingleThreadMultiplexSourceReaderBase<
        RowData, BucketTaggedRow, OdpsSourceSplit, OdpsSourceSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(BucketAwareSourceReader.class);

    public BucketAwareSourceReader(
            Supplier<SplitReader<RowData, OdpsSourceSplit>> splitReaderSupplier,
            RecordEmitter<RowData, BucketTaggedRow, OdpsSourceSplitState> recordEmitter,
            Configuration config,
            SourceReaderContext context) {
        super(splitReaderSupplier, recordEmitter, config, context);
    }

    @Override
    public void start() {
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected void onSplitFinished(Map<String, OdpsSourceSplitState> finishedSplitIds) {
        LOG.info("onSplitFinished {}", finishedSplitIds.keySet());
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected OdpsSourceSplitState initializedState(OdpsSourceSplit split) {
        return new OdpsSourceSplitState(split);
    }

    @Override
    protected OdpsSourceSplit toSplitType(String splitId, OdpsSourceSplitState state) {
        return state.toSourceSplit();
    }
}
