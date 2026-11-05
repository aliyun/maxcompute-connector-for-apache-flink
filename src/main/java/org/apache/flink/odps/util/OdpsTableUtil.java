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

package org.apache.flink.odps.util;

import com.aliyun.odps.Column;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoParser;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;

/**
 * Utils for Odps table.
 */
public class OdpsTableUtil {

    private OdpsTableUtil() {
    }

    /**
     * Create a Flink's TableSchema from Odps table's columns and partition keys.
     */
    public static TableSchema createTableSchema(List<Column> cols, List<Column> partitionKeys) {
        return createTableSchema(cols, partitionKeys, null);
    }

    /**
     * Create a Flink's TableSchema from Odps table's columns and partition keys.
     */
    public static TableSchema createTableSchema(List<Column> cols, List<Column> partitionKeys, List<String> primaryKeys) {
        List<Column> allCols = new ArrayList<>(cols);
        allCols.addAll(partitionKeys);

        String[] colNames = new String[allCols.size()];
        DataType[] colTypes = new DataType[allCols.size()];

        for (int i = 0; i < allCols.size(); i++) {
            Column col = allCols.get(i);

            colNames[i] = col.getName();
            colTypes[i] = OdpsTypeUtil.toFlinkType(col.getTypeInfo());
            if (col.isNullable() && (primaryKeys == null || !primaryKeys.contains(col.getName()))) {
                colTypes[i] = colTypes[i].nullable();
            } else {
                colTypes[i] = colTypes[i].notNull();
            }
        }

        TableSchema.Builder builder = TableSchema.builder()
                .fields(colNames, colTypes);
        if (primaryKeys != null && !primaryKeys.isEmpty()) {
            builder.primaryKey(primaryKeys.toArray(new String[0]));
        }
        return builder.build();
    }

    public static ResolvedSchema createResolvedSchema(List<Column> cols, List<Column> partitionKeys, List<String> primaryKeys) {
        List<Column> allCols = new ArrayList<>(cols);
        allCols.addAll(partitionKeys);

        String[] colNames = new String[allCols.size()];
        DataType[] colTypes = new DataType[allCols.size()];

        for (int i = 0; i < allCols.size(); i++) {
            Column col = allCols.get(i);

            colNames[i] = col.getName();
            colTypes[i] = OdpsTypeUtil.toFlinkType(col.getTypeInfo());
            if (col.isNullable() && (primaryKeys == null || !primaryKeys.contains(col.getName()))) {
                colTypes[i] = colTypes[i].nullable();
            } else {
                colTypes[i] = colTypes[i].notNull();
            }
        }

        List<org.apache.flink.table.catalog.Column> columns = IntStream.range(0, colNames.length)
                .mapToObj((idx) -> org.apache.flink.table.catalog.Column.physical(colNames[idx], colTypes[idx]))
                .collect(Collectors.toList());

        UniqueConstraint uniqueConstraint = null;
        if (primaryKeys != null && !primaryKeys.isEmpty()) {
            uniqueConstraint = UniqueConstraint.primaryKey(UUID.randomUUID().toString(), primaryKeys);
        }

        return new ResolvedSchema(columns, Collections.emptyList(), uniqueConstraint);
    }

    /**
     * Create Odps columns from Flink TableSchema.
     */
    public static List<Column> createOdpsColumns(TableSchema schema) {
        String[] fieldNames = schema.getFieldNames();
        DataType[] fieldTypes = schema.getFieldDataTypes();
        List<Column> columns = new ArrayList<>(fieldNames.length);

        for (int i = 0; i < fieldNames.length; i++) {
            Column column = new Column(fieldNames[i], OdpsTypeUtil.toOdpsTypeInfo(fieldTypes[i]));
            column.setNullable(fieldTypes[i].getLogicalType().isNullable());
            columns.add(column);
        }

        return columns;
    }

    public static DataType toRowDataType(List<Column> columns) {
        final DataTypes.Field[] fields =
                columns.stream()
                        .map(column -> FIELD(column.getName(),
                                OdpsTypeUtil.toFlinkType(column.getTypeInfo())))
                        .toArray(DataTypes.Field[]::new);
        return ROW(fields).notNull();
    }

    public static TypeInfo getTypeInfoFromString(String typeName) {
        TypeInfo typeInfo = TypeInfoParser.getTypeInfoFromTypeString(typeName);
        if (typeInfo == null) {
            throw new RuntimeException("parse odps type info fail");
        }
        return typeInfo;
    }

    /**
     * Check whether the Columns type of the two RowTypes are consistent, ignoring the Column Name and Nullable
     * all timestamp type will consider the same
     * @param partialRowType the RowType of Flink Table, may contain only partial columns of actual Odps table.
     * @param fullRowType the RowType of Odps Table.
     * @throws IllegalArgumentException if column types are inconsistent
     */
    public static void checkPartialSchema(RowType partialRowType, RowType fullRowType) {
        List<RowType.RowField> fieldsA = partialRowType.getFields();
        List<RowType.RowField> fieldsB = fullRowType.getFields();

        for (int i = 0; i < fieldsA.size(); i++) {
            int indexInFullRowType = fullRowType.getFieldIndex(fieldsA.get(i).getName());
            if (indexInFullRowType < 0) {
                throw new IllegalArgumentException(
                        "Field " + fieldsA.get(i).getName() + " is missing in full RowType : " + fullRowType.getFieldNames());
            }

            LogicalType typeA = fieldsA.get(i).getType();
            LogicalType typeB = fieldsB.get(indexInFullRowType).getType();
            // use type root to ignore nullable
            if (!typeA.getTypeRoot().equals(typeB.getTypeRoot())) {
                // ignore TIMESTAMP_LTZ
                if (typeA.asSummaryString().startsWith("TIMESTAMP") && typeB.asSummaryString().startsWith(
                    "TIMESTAMP")) {
                    continue;
                }
                throw new IllegalArgumentException(
                    "Field types mismatch at position " + i + ": " + typeA.asSummaryString()
                    + " is not compatible with " + typeB.asSummaryString() + ".");
            }
        }
    }
}
