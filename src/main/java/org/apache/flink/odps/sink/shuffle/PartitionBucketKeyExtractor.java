/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work by the regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

package org.apache.flink.odps.sink.shuffle;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.odps.sink.partition.PrimaryKeySelector;
import org.apache.flink.odps.sink.utils.RowDataProjection;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.aliyun.odps.Column;

/**
 * Extracts a {@link PartitionBucketKey} from a {@link RowData}.
 * <p>
 * This combines:
 * <ul>
 *   <li>Partition spec extraction (from partition columns in the RowData)</li>
 *   <li>Bucket ID computation (from primary key hash % bucketNum, reusing {@link PrimaryKeySelector})</li>
 * </ul>
 * <p>
 * The resulting PartitionBucketKey is used as the routing key in the dynamic shuffle.
 */
public class PartitionBucketKeyExtractor implements KeySelector<RowData, PartitionBucketKey>, Serializable {

    private static final long serialVersionUID = 1L;

    /** Reuses PrimaryKeySelector for bucket ID computation */
    private final PrimaryKeySelector primaryKeySelector;

    /** Projection for partition column values */
    private final RowDataProjection partitionProjection;

    /** Partition column names (for building partition spec string) */
    private final String[] partitionColumnNames;

    /** Indices of dynamic partition columns (not covered by static partition prefix) */
    private final int[] dynamicPartitionProjectionIndices;

    /** Names of dynamic partition columns */
    private final String[] dynamicPartitionColumnNames;

    /** Static partition spec prefix, e.g. "ds=20240101/" */
    private final String staticPartitionPrefix;

    /** Default value for null partition columns */
    private final String partitionDefaultValue;

    /** Number of buckets */
    private final int bucketNum;

    /** Whether this table has partition keys */
    private final boolean hasPartitions;

    public PartitionBucketKeyExtractor(
            List<Column> odpsColumns,
            List<String> primaryKeys,
            List<String> partitionColumnNames,
            int bucketNum,
            Map<String, String> staticPartitionSpec,
            String partitionDefaultValue,
            RowType flinkSchema) {

        this.primaryKeySelector = new PrimaryKeySelector(odpsColumns, primaryKeys, bucketNum, flinkSchema);
        this.bucketNum = bucketNum;
        this.partitionDefaultValue = partitionDefaultValue != null ? partitionDefaultValue : "__HIVE_DEFAULT_PARTITION__";

        this.hasPartitions = partitionColumnNames != null && !partitionColumnNames.isEmpty();

        if (hasPartitions) {
            // Build projection for all partition columns
            List<String> allFields = flinkSchema.getFieldNames();
            int[] partitionPositions = partitionColumnNames.stream()
                    .mapToInt(allFields::indexOf)
                    .toArray();
            this.partitionProjection = RowDataProjection.instance(flinkSchema, partitionPositions);
            this.partitionColumnNames = partitionColumnNames.toArray(new String[0]);

            // Determine dynamic partition columns (those not in static partition spec)
            List<Integer> dynamicIndices = new ArrayList<>();
            List<String> dynamicNames = new ArrayList<>();
            for (int i = 0; i < partitionColumnNames.size(); i++) {
                if (staticPartitionSpec == null || !staticPartitionSpec.containsKey(partitionColumnNames.get(i))) {
                    dynamicIndices.add(i);
                    dynamicNames.add(partitionColumnNames.get(i));
                }
            }
            this.dynamicPartitionProjectionIndices = dynamicIndices.stream().mapToInt(Integer::intValue).toArray();
            this.dynamicPartitionColumnNames = dynamicNames.toArray(new String[0]);

            // Build static partition prefix
            if (staticPartitionSpec != null && !staticPartitionSpec.isEmpty()) {
                StringBuilder sb = new StringBuilder();
                for (String colName : partitionColumnNames) {
                    if (staticPartitionSpec.containsKey(colName)) {
                        if (sb.length() > 0) {
                            sb.append('/');
                        }
                        sb.append(colName).append('=').append(staticPartitionSpec.get(colName));
                    }
                }
                this.staticPartitionPrefix = sb.toString();
            } else {
                this.staticPartitionPrefix = "";
            }
        } else {
            this.partitionProjection = null;
            this.partitionColumnNames = new String[0];
            this.dynamicPartitionProjectionIndices = new int[0];
            this.dynamicPartitionColumnNames = new String[0];
            this.staticPartitionPrefix = "";
        }
    }

    /**
     * Extract the (partitionSpec, bucketId) key from the given RowData.
     * Implements {@link KeySelector} for use with {@code partitionCustom}.
     */
    @Override
    public PartitionBucketKey getKey(RowData record) {
        // Compute bucket ID from primary key (same logic as PrimaryKeySelector)
        int bucketId = primaryKeySelector.getKey(record);

        // Compute partition spec string
        String partitionSpec = extractPartitionSpec(record);

        return new PartitionBucketKey(partitionSpec, bucketId);
    }

    /**
     * Extract partition spec from the RowData.
     * Returns a string like "ds=20240101/region=cn" or "" for non-partitioned tables.
     */
    private String extractPartitionSpec(RowData record) {
        if (!hasPartitions || partitionProjection == null) {
            return "";
        }

        Object[] values = partitionProjection.projectAsValues(record);

        StringBuilder sb = new StringBuilder();

        // Add static partition prefix first
        if (staticPartitionPrefix != null && !staticPartitionPrefix.isEmpty()) {
            sb.append(staticPartitionPrefix);
        }

        // Add dynamic partition values
        for (int i = 0; i < dynamicPartitionProjectionIndices.length; i++) {
            int valueIndex = dynamicPartitionProjectionIndices[i];
            String colName = dynamicPartitionColumnNames[i];

            if (sb.length() > 0) {
                sb.append('/');
            }

            Object value = values[valueIndex];
            String valueStr = value != null ? value.toString() : partitionDefaultValue;
            sb.append(colName).append('=').append(valueStr);
        }

        return sb.toString();
    }

    public int getBucketNum() {
        return bucketNum;
    }

    public boolean hasPartitions() {
        return hasPartitions;
    }
}
