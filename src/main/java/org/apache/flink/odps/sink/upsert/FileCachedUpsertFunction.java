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

package org.apache.flink.odps.sink.upsert;

import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.TableIdentifier;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.sink.common.OdpsWriteOptions;
import org.apache.flink.odps.sink.common.WriteOperationType;
import org.apache.flink.odps.sink.insert.FileCachedInsertFunction;
import org.apache.flink.odps.sink.partition.PartitionAssigner;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

public class FileCachedUpsertFunction extends FileCachedInsertFunction {

    public FileCachedUpsertFunction(Configuration config,
                                    OdpsConf odpsConf,
                                    TableIdentifier identifier,
                                    String partition,
                                    DataSchema schema,
                                    boolean isDynamicPartition,
                                    boolean isGroupPartition,
                                    OdpsWriteOptions writeOptions,
                                    PartitionAssigner<RowData> partitionAssigner,
                                    WriteOperationType writeOperationType,
                                    RowType rowType) {
        super(config, odpsConf, identifier, partition, schema,
                isDynamicPartition, isGroupPartition, writeOptions, partitionAssigner, writeOperationType, rowType);
    }
}
