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

package org.apache.flink.odps.source;

import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.odps.source.partition.LatestPartitionReader;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.CompressionCodec;
import com.aliyun.odps.table.configuration.IncrementalOptions;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.odps.schema.OdpsColumn;
import org.apache.flink.odps.source.enumerator.ContinuousOdpsEnumerator;
import org.apache.flink.odps.source.enumerator.ContinuousOdpsSplitEnumState;
import org.apache.flink.odps.source.enumerator.ContinuousOdpsSplitEnumStateSerializer;
import org.apache.flink.odps.source.split.OdpsSourceSplit;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.table.data.RowData;

import java.time.ZoneId;
import java.util.Collections;
import java.util.List;

public class ContinuousOdpsSource extends OdpsSourceBase<ContinuousOdpsSplitEnumState> {

    private static final long serialVersionUID = 1L;

    // partitions left after partition push-down or pruned by specific partitions for bounded mode.
    private final List<String> prunedPartitions;


    private final OdpsSplitStrategy odpsSplitStrategy;

    private final long splitByteSize;

    private final int splitMaxPerTask;

    private final OdpsStartupMode odpsStartupMode;

    private final long discoveryInterval;

    public ContinuousOdpsSource(
            OdpsConf odpsConf,
            boolean isPartitionedTable,
            TableIdentifier odpsTable,
            List<String> prunedPartitions,
            OdpsColumn[] selectedColumns,
            OdpsColumn[] selectedPtColumns,
            TypeInformation<RowData> producedTypeInfo,
            long fetchCount,
            CompressionCodec codec,
            OdpsSplitStrategy odpsSplitStrategy,
            long splitByteSize,
            int splitMaxPerTask,
            long discoveryInterval,
            OdpsStartupMode odpsStartupMode,
            IncrementalOptions incrementalOptions,
            ZoneId zoneId) {
        super(
                odpsConf,
                odpsTable,
                selectedColumns,
                selectedPtColumns,
                producedTypeInfo,
                fetchCount,
                codec,
                incrementalOptions,
                zoneId);
        if (odpsConf.getPropertyOrDefault("scan.partition.latest", false)) {
            if (!isPartitionedTable || !prunedPartitions.isEmpty()
                    || incrementalOptions.getIncrementalMode() != IncrementalOptions.IncrementalMode.CDC
                    || (odpsStartupMode != OdpsStartupMode.LATEST
                        && odpsStartupMode != OdpsStartupMode.FROM_SNAPSHOT)) {
                throw new IllegalArgumentException("Latest-partition requires an unpruned partitioned CDC "
                        + "source with latest or from-snapshot startup");
            }
            if (!"partition-visible".equals(odpsConf.getProperty("scan.partition.ready-mode"))) {
                throw new IllegalArgumentException("Latest-partition requires an explicit partition-visible contract");
            }
        }
        if (isPartitionedTable) {
            this.prunedPartitions = prunedPartitions;
        } else {
            // set a global empty partition
            this.prunedPartitions = Collections.emptyList();
        }
        this.odpsSplitStrategy = odpsSplitStrategy;
        this.splitByteSize = splitByteSize;
        this.splitMaxPerTask = splitMaxPerTask;
        this.odpsStartupMode = odpsStartupMode;
        this.discoveryInterval = discoveryInterval;
    }

    @Override
    public SourceReader<RowData, OdpsSourceSplit> createReader(
            SourceReaderContext context) {
        if (odpsConf.getPropertyOrDefault("scan.partition.latest", false)) {
            return new LatestPartitionReader(
                    context, () -> super.createReader(context));
        }
        return super.createReader(context);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<OdpsSourceSplit, ContinuousOdpsSplitEnumState> createEnumerator(
            SplitEnumeratorContext<OdpsSourceSplit> context) {
        return restoreEnumerator(context, null);
    }

    @Override
    public SplitEnumerator<OdpsSourceSplit, ContinuousOdpsSplitEnumState> restoreEnumerator(
            SplitEnumeratorContext<OdpsSourceSplit> context, ContinuousOdpsSplitEnumState enumState) {
        return new ContinuousOdpsEnumerator(
                context,
                odpsConf,
                odpsTable,
                selectedColumnNames,
                selectedPtColumnNames,
                prunedPartitions,
                odpsSplitStrategy,
                splitByteSize,
                splitMaxPerTask,
                discoveryInterval,
                odpsStartupMode,
                incrementalOptions,
                enumState);
    }

    @Override
    public SimpleVersionedSerializer<ContinuousOdpsSplitEnumState> getEnumeratorCheckpointSerializer() {
        return new ContinuousOdpsSplitEnumStateSerializer();
    }
}
