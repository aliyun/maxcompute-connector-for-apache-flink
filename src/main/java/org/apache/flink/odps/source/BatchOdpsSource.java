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

import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.CompressionCodec;
import com.aliyun.odps.table.configuration.IncrementalOptions;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.odps.schema.OdpsColumn;
import org.apache.flink.odps.source.enumerator.BatchOdpsSplitEnumState;
import org.apache.flink.odps.source.enumerator.BatchOdpsSplitEnumStateSerializer;
import org.apache.flink.odps.source.enumerator.BatchOdpsSplitEnumerator;
import org.apache.flink.odps.source.split.OdpsSourceSplit;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.table.data.RowData;

import java.time.ZoneId;
import java.util.Collections;
import java.util.List;

/**
 * FLIP-27 ODPS source for batch jobs.
 */
public class BatchOdpsSource extends OdpsSourceBase<BatchOdpsSplitEnumState> {

    private static final long serialVersionUID = 1L;

    // partitions left after partition push-down or pruned by specific partitions for bounded mode.
    private final List<String> prunedPartitions;

    private final boolean dynamicLoadBalance;

    private final OdpsSplitStrategy odpsSplitStrategy;

    private final long splitByteSize;

    public BatchOdpsSource(
            OdpsConf odpsConf,
            boolean isPartitionedTable,
            TableIdentifier odpsTable,
            List<String> prunedPartitions,
            OdpsColumn[] selectedColumns,
            OdpsColumn[] selectedPtColumns,
            TypeInformation<RowData> producedTypeInfo,
            long fetchCount,
            boolean dynamicLoadBalance,
            OdpsSplitStrategy odpsSplitStrategy,
            CompressionCodec codec,
            long splitByteSize,
            IncrementalOptions incrementalOptions,
            ZoneId zoneId) {
        super(odpsConf,
                odpsTable,
                selectedColumns,
                selectedPtColumns,
                producedTypeInfo,
                fetchCount,
                codec,
                incrementalOptions,
                zoneId);
        if (isPartitionedTable) {
            this.prunedPartitions = prunedPartitions;
        } else {
            // set a global empty partition
            this.prunedPartitions = Collections.emptyList();
        }
        this.dynamicLoadBalance = dynamicLoadBalance;
        this.odpsSplitStrategy = odpsSplitStrategy;
        this.splitByteSize = splitByteSize;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<OdpsSourceSplit, BatchOdpsSplitEnumState> createEnumerator(
            SplitEnumeratorContext<OdpsSourceSplit> context) {
        return new BatchOdpsSplitEnumerator(
                context,
                odpsConf,
                odpsTable,
                selectedColumnNames,
                selectedPtColumnNames,
                prunedPartitions,
                dynamicLoadBalance,
                odpsSplitStrategy,
                splitByteSize,
                incrementalOptions);
    }

    @Override
    public SplitEnumerator<OdpsSourceSplit, BatchOdpsSplitEnumState> restoreEnumerator(
            SplitEnumeratorContext<OdpsSourceSplit> context, BatchOdpsSplitEnumState enumState) {
        return new BatchOdpsSplitEnumerator(
                context,
                odpsConf,
                odpsTable,
                selectedColumnNames,
                selectedPtColumnNames,
                prunedPartitions,
                enumState,
                dynamicLoadBalance,
                odpsSplitStrategy,
                splitByteSize,
                incrementalOptions);
    }

    @Override
    public SimpleVersionedSerializer<BatchOdpsSplitEnumState> getEnumeratorCheckpointSerializer() {
        return new BatchOdpsSplitEnumStateSerializer();
    }
}