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

package org.apache.flink.odps.sink.cache;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.odps.sink.partition.PartitionComputer;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.table.data.RowData;

public class PartitionBucketAssigner implements BucketAssigner<RowData, String> {

    private final PartitionComputer computer;

    public PartitionBucketAssigner(PartitionComputer computer) {
        this.computer = computer;
    }

    @Override
    public String getBucketId(RowData element, Context context) {
        try {
            return OdpsUtils.generatePartition(computer.generatePartValues(element, null));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }

}
