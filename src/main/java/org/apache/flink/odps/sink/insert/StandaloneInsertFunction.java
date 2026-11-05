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

package org.apache.flink.odps.sink.insert;

import java.io.IOException;

import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.write.TableBatchWriteSession;
import com.aliyun.odps.table.write.WriterCommitMessage;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.sink.common.OdpsWriteOptions;
import org.apache.flink.odps.sink.common.WriteOperationType;
import org.apache.flink.odps.sink.partition.PartitionAssigner;
import org.apache.flink.odps.sink.utils.WriterStatus;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author dingxin ()
 */
public class StandaloneInsertFunction extends InsertFunction {
    private static final long serialVersionUID = 100L;
    private static final Logger LOG = LoggerFactory.getLogger(StandaloneInsertFunction.class);

    public StandaloneInsertFunction(Configuration config, OdpsConf odpsConf, TableIdentifier identifier,
        String partition, DataSchema schema, boolean isDynamicPartition, boolean isGroupPartition,
        boolean supportsGrouping, OdpsWriteOptions writeOptions, PartitionAssigner<RowData> partitionAssigner,
        WriteOperationType writeOperationType, RowType rowType) {
        super(config, odpsConf, identifier, partition, schema, isDynamicPartition, isGroupPartition, supportsGrouping,
              writeOptions, partitionAssigner, writeOperationType, rowType);
    }

    @Override
    public void processElement(RowData in, Context ctx, Collector<Object> out) throws Exception {
        validDateRowData(in);

        String currentPartition = "";
        if (!isDynamicPartition) {
            currentPartition = partition;
        } else {
            currentPartition = partitionAssigner.getPartitionSpec(in, null);
        }

        if (!sessionRequest.containsKey(currentPartition)) {
            bootstrapForGroupPartition(currentPartition);
        }

        writeData(currentPartition, in);
    }

    @Override
    protected void snapshotState(long checkpoint) throws IOException {
        LOG.info("Snapshot state for checkpoint: " + checkpoint);
        flushRemaining(inputEnded, checkpoint);
    }

    @Override
    protected void flushRemaining(boolean endInput, long checkpointId) throws IOException {
        commitWriter();
        for (WriterStatus status : statuses) {
            String partitionSpec = status.getPartitionSpec();
            TableBatchWriteSession writeSession = odpsSessionMap.get(partitionSpec);
            WriterCommitMessage writerMessage = status.getWriterMessage();
            writeSession.commit(new WriterCommitMessage[] {writerMessage});
        }
        reset();
        // This mode commits synchronously and receives no coordinator acknowledgement.
        commitConfirming = false;
    }
}
