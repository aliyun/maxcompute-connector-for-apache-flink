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

import java.util.LinkedList;
import java.util.List;

public class DataBucket {
    private final List<DataItem> records;
    private final String partitionPath;

    public DataBucket(String partitionPath) {
        this.records = new LinkedList<>();
        this.partitionPath = partitionPath;
    }

    public void add(DataItem item) {
        this.records.add(item);
    }

    public boolean isEmpty() {
        return this.records.isEmpty();
    }

    public List<DataItem> getRecords() {
        return this.records;
    }

    public String getPartitionPath() {
        return partitionPath;
    }

    public void reset() {
        this.records.clear();
    }
}