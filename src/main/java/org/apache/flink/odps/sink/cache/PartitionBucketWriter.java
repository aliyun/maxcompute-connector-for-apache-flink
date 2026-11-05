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

import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.TableIdentifier;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.odps.sink.common.WriteOperationType;
import org.apache.flink.odps.sink.utils.MemoryUtils;
import org.apache.flink.odps.sink.writer.*;
import org.apache.flink.odps.table.OdpsOptions;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.runtime.io.disk.FileChannelManager;
import org.apache.flink.runtime.io.disk.FileChannelManagerImpl;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;

public class PartitionBucketWriter implements BucketWriter<RowData, String> {

    private final Configuration configuration;
    private final OdpsConf odpsConf;
    private final TableIdentifier identifier;
    private final DataSchema schema;
    private final WriteOperationType type;
    private final int subtaskIndex;
    private final int taskParallel;

    private final FileChannelManager fileChannelManager;
    private final BufferPool bufferPool;
    private final TypeSerializer<RowData> inputSerializer;

    private final String tmpDir;
    private final int readBufferSize;
    private final RowType rowType;

    public PartitionBucketWriter(Configuration configuration,
                                 OdpsConf odpsConf,
                                 TableIdentifier identifier,
                                 DataSchema schema,
                                 WriteOperationType type,
                                 final int subtaskIndex,
                                 final int taskParallel,
                                 final BufferPool bufferPool,
                                 final TypeSerializer<RowData> inputSerializer,
                                 final RowType rowType) {
        this.configuration = configuration;
        this.odpsConf = odpsConf;
        this.identifier = identifier;
        this.schema = schema;
        this.type = type;
        this.subtaskIndex = subtaskIndex;
        this.taskParallel = taskParallel;

        this.tmpDir = configuration.getString(OdpsOptions.SINK_FILE_TMP_DIRS);
        this.fileChannelManager = new FileChannelManagerImpl(new String[]{tmpDir}, "file-cached-data");;
        this.bufferPool = bufferPool;
        this.inputSerializer = inputSerializer;
        this.readBufferSize = MemoryUtils.getPageSize(configuration);
        this.rowType = rowType;
    }

    @Override
    public InProgressFileWriter<RowData, String> openNewInProgressFile(final String bucketID,
                                                                       final long creationTime) throws IOException {
        return new PartitionInProgressFileWriter(bucketID, creationTime, fileChannelManager, bufferPool, inputSerializer,
                configuration.getBoolean(OdpsOptions.SINK_FILE_FLUSH_ALWAYS), tmpDir, readBufferSize);
    }

    @Override
    public DataWriter<RowData> openDataWriter(final String bucketID, final WriterInfo writerInfo) throws IOException {
        PartitionWriterInfo partitionWriterInfo = (PartitionWriterInfo) writerInfo;
        return OdpsWriterFactory.createOdpsTableWriter(configuration,
                odpsConf, identifier, bucketID, schema, type, partitionWriterInfo, subtaskIndex, taskParallel, rowType);
    }

    @Override
    public SimpleVersionedSerializer<InProgressFileWriter.CommittingFile<RowData>> getCommittingFileSerializer() {
        return new PartitionInProgressFileWriter.PartitionCommittingFileSerializer(inputSerializer, tmpDir, readBufferSize);
    }

    @Override
    public SimpleVersionedSerializer<CommitInfo> getCommitInfoSerializer() {
        return PartitionCommitInfo.PartitionCommitInfoSerializer.INSTANCE;
    }

}
