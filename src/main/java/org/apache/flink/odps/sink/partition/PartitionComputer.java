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

package org.apache.flink.odps.sink.partition;

import com.aliyun.odps.Column;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.table.DataSchema;
import org.apache.flink.odps.sink.utils.RowDataProjection;
import org.apache.flink.odps.util.OdpsTableUtil;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.flink.odps.sink.utils.RowDataProjection.getProjection;
import static org.apache.flink.odps.util.OdpsUtils.objToString;

public class PartitionComputer implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String defaultPartValue;
    private final List<String> partitionColumns;
    private final Map<String, String> staticPartitionSpec;

    private final RowDataProjection partitionPathProjection;
    private boolean simplePartitionPath = false;
    private RowData.FieldGetter partitionPathFieldGetter;
    private boolean nonPartitioned;

    public PartitionComputer(RowType odpsSchema,
                             RowType flinkSchema,
                             List<String> partitionColumns,
                             String staticPartition,
                             String defaultPartValue) {

        List<String> fieldNames = odpsSchema.getFieldNames();
        List<LogicalType> fieldTypes = flinkSchema.getChildren();

        this.staticPartitionSpec = new LinkedHashMap<>();
        if (!StringUtils.isNullOrWhitespaceOnly(staticPartition)) {
            PartitionSpec partitionSpec = new PartitionSpec(staticPartition);
            partitionSpec.keys().forEach((key) -> {
                staticPartitionSpec.put(key, partitionSpec.get(key));
            });
        }

        this.partitionColumns = partitionColumns.stream()
                .filter(col -> !staticPartitionSpec.containsKey(col))
                .collect(Collectors.toList());

        if (this.partitionColumns.isEmpty()) {
            this.nonPartitioned = true;
            this.partitionPathProjection = null;
        } else if (this.partitionColumns.size() == 1) {
            this.simplePartitionPath = true;
            int partitionPathIdx = flinkSchema.getFieldIndex(this.partitionColumns.get(0));
            this.partitionPathFieldGetter = RowData.createFieldGetter(fieldTypes.get(partitionPathIdx), partitionPathIdx);
            this.partitionPathProjection = null;
        } else {
            this.partitionPathProjection = getProjection(this.partitionColumns, flinkSchema.getFieldNames(), fieldTypes);
        }

        this.defaultPartValue = defaultPartValue;
    }

    public static PartitionComputer instance(DataSchema dataSchema,
                                             RowType flinkSchema,
                                             String staticPartition,
                                             String defaultPartValue) {
        // TODO: set row type
        List<Column> columnList = dataSchema.getColumns();
        RowType odpsSchema =
            ((RowType)OdpsTableUtil.toRowDataType(columnList).getLogicalType());
        return instance(odpsSchema, flinkSchema, dataSchema.getPartitionKeys(), staticPartition, defaultPartValue);
    }

    public static PartitionComputer instance(RowType odpsSchema,
                                             RowType flinkSchema,
                                             List<String> partitionKeys,
                                             String staticPartition,
                                             String defaultPartValue) {
        return new PartitionComputer(odpsSchema, flinkSchema, partitionKeys, staticPartition, defaultPartValue);
    }

    public LinkedHashMap<String, String> generatePartValues(RowData rowData, PartitionAssigner.Context context) {
        LinkedHashMap<String, String> partSpec = new LinkedHashMap<>(staticPartitionSpec);
        if (!nonPartitioned) {
            if (simplePartitionPath) {
                String partValue = objToString(partitionPathFieldGetter.getFieldOrNull(rowData));
                String partField = partitionColumns.get(0);
                if (partValue == null || partValue.isEmpty()) {
                    partSpec.put(partField, defaultPartValue);
                } else {
                    partSpec.put(partField, partValue);
                }
            } else {
                Object[] partValues = this.partitionPathProjection.projectAsValues(rowData);
                for (int i = 0; i < partitionColumns.size(); i++) {
                    String partField = partitionColumns.get(i);
                    String partValue = objToString(partValues[i]);
                    if (partValue == null || partValue.isEmpty()) {
                        partSpec.put(partField, defaultPartValue);
                    } else {
                        partSpec.put(partField, partValue);
                    }
                }
            }
        }
        return partSpec;
    }
}