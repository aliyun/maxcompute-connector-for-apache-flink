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
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.tunnel.hasher.TypeHasher;
import com.aliyun.odps.type.TypeInfo;
import org.apache.flink.api.java.functions.KeySelector;

import org.apache.flink.odps.util.RowDataToOdpsConverters;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.odps.sink.utils.RowDataProjection.getFieldPositions;

public class PrimaryKeySelector implements KeySelector<RowData, Integer> {

    private final List<OdpsType> keyTypeNames;
    private final int[] hashValues;
    private final int bucketNum;
    private final int keyNum;

    private final RowDataToOdpsConverters.RowDataToPrimaryKeyConverter primaryKeyConverter;

    public PrimaryKeySelector(List<Column> odpsColumnList,
                              List<String> primaryKeys,
                              int bucketNum,
                              RowType flinkSchema) {
        List<String> odpsFieldNames = odpsColumnList.stream().map(Column::getName).collect(Collectors.toList());
        List<LogicalType> flinkFieldTypes = flinkSchema.getChildren();

        Preconditions.checkArgument(bucketNum > 0, "BucketNum is less than 0");
        Preconditions.checkArgument( primaryKeys != null && !primaryKeys.isEmpty(), "Primary keys is empty or null!");

        this.bucketNum = bucketNum;
        this.keyNum = primaryKeys.size();

        int[] fieldPositionsInOdps = getFieldPositions(primaryKeys, odpsFieldNames);
        int[] fieldPositionsInFlink = getFieldPositions(primaryKeys, flinkSchema.getFieldNames());
        LogicalType[] keyFieldTypes = Arrays.stream(fieldPositionsInFlink).mapToObj(flinkFieldTypes::get).toArray(LogicalType[]::new);
        TypeInfo[] keyTypeInfos = Arrays.stream(fieldPositionsInOdps)
                .mapToObj(index -> odpsColumnList.get(index).getTypeInfo())
                .toArray(TypeInfo[]::new);

        this.primaryKeyConverter = RowDataToOdpsConverters.createPrimaryKeyConverter(keyFieldTypes, fieldPositionsInFlink, keyTypeInfos);
        this.keyTypeNames = Arrays.stream(fieldPositionsInOdps).mapToObj(pos -> odpsColumnList.get(pos)
                .getTypeInfo().getOdpsType()).collect(Collectors.toList());

        this.hashValues = new int[keyNum];
    }

    @Override
    public Integer getKey(RowData rowData) {
        Object[] keyValues = new Object[keyNum];
        this.primaryKeyConverter.convert(rowData, keyValues);
        for (int i = 0; i < keyValues.length; i++) {
            if (keyValues[i] == null) {
                hashValues[i] = 0;
            } else {
                hashValues[i] = TypeHasher.hash(keyTypeNames.get(i), keyValues[i]);
            }
        }
        return TypeHasher.CombineHashVal(hashValues) % bucketNum;
    }
}
