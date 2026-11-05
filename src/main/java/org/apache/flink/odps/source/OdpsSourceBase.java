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
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.odps.schema.OdpsColumn;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.odps.source.reader.MetadataRecordEmitter;
import org.apache.flink.odps.source.reader.OdpsRecordEmitter;
import org.apache.flink.odps.source.reader.OdpsSourceReader;
import org.apache.flink.odps.source.reader.OdpsSourceSplitReader;
import org.apache.flink.odps.source.split.OdpsSourceSplit;
import org.apache.flink.odps.source.split.OdpsSourceSplitSerializer;
import org.apache.flink.odps.source.split.OdpsSourceSplitState;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsTypeUtil;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * Abstract FLIP-27 ODPS source.
 */
public abstract class OdpsSourceBase<CheckpointT>
        implements Source<RowData, OdpsSourceSplit, CheckpointT>, ResultTypeQueryable<RowData> {

    protected final OdpsConf odpsConf;
    protected final TableIdentifier odpsTable;
    protected final OdpsColumn[] selectedColumns;
    protected final List<String> selectedColumnNames;
    protected final OdpsColumn[] selectedPtColumns;
    protected final List<String> selectedPtColumnNames;
    protected final IncrementalOptions incrementalOptions;

    private final TypeInformation<RowData> producedTypeInfo;
    private final long fetchCount;

    private final CompressionCodec codec;

    private final ZoneId zoneId;

    private List<String> metadataKeys = Collections.emptyList();

    public void setMetadataKeys(List<String> metadataKeys) {
        this.metadataKeys = metadataKeys;
    }

    public OdpsSourceBase(
            OdpsConf odpsConf,
            TableIdentifier odpsTable,
            OdpsColumn[] selectedColumns,
            OdpsColumn[] selectedPtColumns,
            TypeInformation<RowData> producedTypeInfo,
            long fetchCount,
            CompressionCodec codec,
            IncrementalOptions incrementalOptions,
            ZoneId zoneId) {
        this.odpsConf = odpsConf;
        this.odpsTable = odpsTable;
        this.producedTypeInfo = producedTypeInfo;
        this.selectedColumns = selectedColumns;
        this.selectedPtColumns = selectedPtColumns == null ? new OdpsColumn[0] : selectedPtColumns;
        this.selectedColumnNames = OdpsUtils.getColumnNames(selectedColumns);
        if (selectedPtColumns == null) {
            this.selectedPtColumnNames = Collections.emptyList();
        } else {
            this.selectedPtColumnNames = OdpsUtils.getColumnNames(selectedPtColumns);
        }
        this.fetchCount = fetchCount;
        this.codec = codec;
        this.zoneId = zoneId;
        this.incrementalOptions = incrementalOptions;
    }

    @Override
    public SourceReader<RowData, OdpsSourceSplit> createReader(SourceReaderContext context) {
        Supplier<SplitReader<RowData, OdpsSourceSplit>> splitReaderSupplier =
                newSplitReaderSupplier();
        RecordEmitter<RowData, RowData, OdpsSourceSplitState> emitter =
                metadataKeys.isEmpty()
                        ? new OdpsRecordEmitter()
                        : new MetadataRecordEmitter(metadataKeys);
        Configuration config = context.getConfiguration();
        return new OdpsSourceReader(splitReaderSupplier, emitter, config, context);
    }

    public Supplier<SplitReader<RowData, OdpsSourceSplit>> newSplitReaderSupplier() {
        return () ->
                new OdpsSourceSplitReader(
                        odpsConf,
                        buildRowType(selectedColumns, selectedPtColumns),
                        fetchCount,
                        codec,
                        zoneId);
    }

    @Override
    public SimpleVersionedSerializer<OdpsSourceSplit> getSplitSerializer() {
        return new OdpsSourceSplitSerializer();
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }

    private RowType buildRowType(OdpsColumn[] selectedColumns, OdpsColumn[] selectedPtColumns) {
        // TODO: column project
        DataType[] dataTypes = new DataType[selectedColumns.length + selectedPtColumns.length];
        String[] fieldNames = new String[selectedColumns.length + selectedPtColumns.length];

        int fieldPos = 0;
        for (OdpsColumn column : selectedColumns) {
            dataTypes[fieldPos] = OdpsTypeUtil.toFlinkType(column.getTypeInfo());
            fieldNames[fieldPos] = column.getName();
            fieldPos++;
        }
        for (OdpsColumn column : selectedPtColumns) {
            dataTypes[fieldPos] = OdpsTypeUtil.toFlinkType(column.getTypeInfo());
            fieldNames[fieldPos] = column.getName();
            fieldPos++;
        }

        LogicalType[] types =
                Arrays.stream(dataTypes).map(DataType::getLogicalType).toArray(LogicalType[]::new);
        return RowType.of(types, fieldNames);
    }
}
