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

package org.apache.flink.odps.sink.partition;

import com.aliyun.odps.Column;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.odps.util.OdpsTableUtil;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;

public class PartitionSelector implements KeySelector<RowData, String> {

    private final PartitionComputer partitionComputer;

    public PartitionSelector(List<Column> columnList,
                             List<String> partitionColumns,
                             String staticPartition,
                             String defaultPartValue,
                             RowType flinkSchema) {
        RowType rowType =
                ((RowType) OdpsTableUtil.toRowDataType(columnList).getLogicalType());
        this.partitionComputer = PartitionComputer.instance(rowType, flinkSchema, partitionColumns, staticPartition,
                                                            defaultPartValue);
    }

    @Override
    public String getKey(RowData rowData) throws Exception {
        return OdpsUtils.generatePartition(
                partitionComputer.generatePartValues(rowData, null));
    }
}
