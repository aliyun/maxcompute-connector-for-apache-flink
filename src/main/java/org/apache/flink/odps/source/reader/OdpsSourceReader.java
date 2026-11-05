/*
 * licensed to the apache software foundation (asf) under one
 * or more contributor license agreements.  see the notice file
 * distributed with this work for additional information
 * regarding copyright ownership.  the asf licenses this file
 * to you under the apache license, version 2.0 (the
 * "license"); you may not use this file except in compliance
 * with the license.  you may obtain a copy of the license at
 *
 *     http://www.apache.org/licenses/license-2.0
 *
 * unless required by applicable law or agreed to in writing, software
 * distributed under the license is distributed on an "as is" basis,
 * without warranties or conditions of any kind, either express or implied.
 * see the license for the specific language governing permissions and
 * limitations under the license.
 */

package org.apache.flink.odps.source.reader;

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
 * The source reader for odps partitions.
 */
public class OdpsSourceReader
        extends SingleThreadMultiplexSourceReaderBase<
        RowData, RowData, OdpsSourceSplit, OdpsSourceSplitState> {
    private static final Logger LOGGER = LoggerFactory.getLogger(OdpsSourceReader.class);

    public OdpsSourceReader(
            Supplier<SplitReader<RowData, OdpsSourceSplit>> splitReaderSupplier,
            RecordEmitter<RowData, RowData, OdpsSourceSplitState> recordEmitter,
            Configuration config,
            SourceReaderContext context) {
        super(splitReaderSupplier, recordEmitter, config, context);
    }

    @Override
    public void start() {
        // request a split only if no splits are assigned during the checkpoint restore
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected void onSplitFinished(Map<String, OdpsSourceSplitState> finishedSplitIds) {
        LOGGER.info("onSplitFinished " + finishedSplitIds);

        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected OdpsSourceSplitState initializedState(OdpsSourceSplit odpsSourceSplit) {
        return new OdpsSourceSplitState(odpsSourceSplit);
    }

    @Override
    protected OdpsSourceSplit toSplitType(String splitId, OdpsSourceSplitState odpsSourceSplitState) {
        return odpsSourceSplitState.toSourceSplit();
    }
}
