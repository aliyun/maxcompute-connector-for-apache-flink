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

package org.apache.flink.odps.sink.utils;

import com.aliyun.odps.data.ArrayRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

public class DataItem {

    private final String partition; // record partition
    private ArrayRecord data; // record payload
    private RowKind rowKind; // operation

    private RowData rowData;

    public DataItem(String partition, RowData data) {
        this.partition = partition;
        this.rowData = data;
    }

    public DataItem(String partition, ArrayRecord data, RowKind rowKind) {
        this.partition = partition;
        this.data = data;
        this.rowKind = rowKind;
    }

    public String getPartition() {
        return partition;
    }

    public ArrayRecord getData() {
        return data;
    }

    public RowKind getRowKind() {
        if (rowKind != null) {
            return rowKind;
        }
        if (rowData != null) {
            return rowData.getRowKind();
        }
        throw new IllegalArgumentException("Get row kind failed!");
    }

    public RowData getRowData() {
        return rowData;
    }
}