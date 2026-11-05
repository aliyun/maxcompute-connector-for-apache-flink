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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.odps.sink.writer.OdpsUpsertWriter;
import org.apache.flink.runtime.io.disk.FileChannelManager;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkState;

public class PartitionInProgressFileWriter implements InProgressFileWriter<RowData, String> {

    private final String bucketID;

    private final long creationTime;

    private long lastUpdateTime;

    private BufferPool bufferPool;

    private SubPartition subPartition;

    private TypeSerializer<RowData> inputSerializer;

    private DataOutputSerializer serializer;

    private BufferBuilder currentBufferBuilder;

    private Counter numBytesOut = new SimpleCounter();

    private Counter numBuffersOut = new SimpleCounter();

    private Counter numRowOut = new SimpleCounter();

    private boolean isFinished;

    private Path path;

    // private final static int READ_BUFFER_SIZE = 2048 * 1024;

    private final boolean flushAlways;

    private final String tmpDir;
    private static final Logger LOG = LoggerFactory.getLogger(PartitionInProgressFileWriter.class);

    public PartitionInProgressFileWriter(final String bucketID,
                                         final long createTime,
                                         final FileChannelManager fileChannelManager,
                                         final BufferPool bufferPool,
                                         final TypeSerializer<RowData> inputSerializer,
                                         final boolean flushAlways,
                                         final String tmpDir,
                                         final int readBufferSize) throws IOException {
        this.bucketID = bucketID;
        this.creationTime = createTime;
        this.lastUpdateTime = createTime;
        this.inputSerializer = inputSerializer;
        this.serializer = new DataOutputSerializer(2048);
        this.bufferPool = bufferPool;
        final File spillFile = fileChannelManager.createChannel().getPathFile();
        this.path = new Path(spillFile.toURI());
        this.subPartition = SubPartition.createWithFileChannel(spillFile, readBufferSize, false);
        this.isFinished = false;
        this.flushAlways = flushAlways;
        this.tmpDir = tmpDir;
    }

    public void markWrite(long now) {
        this.lastUpdateTime = now;
    }

    public void write(final RowData element, final long currentTime) throws IOException {
        numRowOut.inc();
        ByteBuffer buffer = serializeRecord(serializer, element);
        emitRecord(buffer);
        markWrite(currentTime);
        if (flushAlways) {
            flush();
        }
    }

    public void flush() {
        finishUnicastBufferBuilder();
        subPartition.flush();
    }

    @Override
    public PendingFile<RowData> closeForUpload() throws IOException {
        checkInProduceState();
        finishUnicastBufferBuilder();

        subPartition.finish();
        isFinished = true;
        return new PartitionPendingFile(path, getSize(), numRowOut.getCount(), subPartition, inputSerializer, tmpDir);
    }

    @Override
    public void dispose() throws IOException {
        if (currentBufferBuilder != null) {
            currentBufferBuilder.close();
        }
        subPartition.release();
    }

    @Override
    public String getBucketId() {
        return bucketID;
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    @Override
    public long getSize() throws IOException {
        return numBytesOut.getCount();
    }

    private ByteBuffer serializeRecord(DataOutputSerializer serializer, RowData record) throws IOException {
        // the initial capacity should be no less than 4 bytes
        serializer.setPositionUnsafe(4);

        // write data
        inputSerializer.serialize(record, serializer);

        // write length
        serializer.writeIntUnsafe(serializer.length() - 4, 0);

        return serializer.wrapAsByteBuffer();
    }

    private void emitRecord(ByteBuffer record) throws IOException {
        BufferBuilder buffer = appendUnicastDataForNewRecord(record);

        while (record.hasRemaining()) {
            // full buffer, partial record
            finishUnicastBufferBuilder();
            buffer = appendUnicastDataForRecordContinuation(record);
        }

        if (buffer.isFull()) {
            // full buffer, full record
            finishUnicastBufferBuilder();
        }

        // partial buffer, full record
    }

    private BufferBuilder appendUnicastDataForNewRecord(final ByteBuffer record) throws IOException {
        BufferBuilder buffer = currentBufferBuilder;

        if (buffer == null) {
            buffer = requestNewUnicastBufferBuilder();
            subPartition.add(buffer.createBufferConsumerFromBeginning(), 0);
        }

        buffer.appendAndCommit(record);

        return buffer;
    }

    private void finishUnicastBufferBuilder() {
        final BufferBuilder bufferBuilder = currentBufferBuilder;
        if (bufferBuilder != null) {
            numBytesOut.inc(bufferBuilder.finish());
            numBuffersOut.inc();
            currentBufferBuilder = null;
            bufferBuilder.close();
        }
    }

    private BufferBuilder appendUnicastDataForRecordContinuation(
            final ByteBuffer remainingRecordBytes)
            throws IOException {
        final BufferBuilder buffer = requestNewUnicastBufferBuilder();
        // !! Be aware, in case of partialRecordBytes != 0, partial length and data has to
        // `appendAndCommit` first
        // before consumer is created. Otherwise it would be confused with the case the buffer
        // starting
        // with a complete record.
        // !! The next two lines can not change order.
        final int partialRecordBytes = buffer.appendAndCommit(remainingRecordBytes);
        subPartition.add(buffer.createBufferConsumerFromBeginning(), partialRecordBytes);

        return buffer;
    }

    private BufferBuilder requestNewUnicastBufferBuilder()
            throws IOException {
        checkInProduceState();
        final BufferBuilder bufferBuilder = requestNewBufferBuilderFromPool();
        currentBufferBuilder = bufferBuilder;

        return bufferBuilder;
    }

    private void checkInProduceState() throws IllegalStateException {
        checkState(!isFinished, "Partition already finished.");
    }

    private BufferBuilder requestNewBufferBuilderFromPool()
            throws IOException {

        BufferBuilder bufferBuilder = bufferPool.requestBufferBuilder();
        if (bufferBuilder != null) {
            return bufferBuilder;
        }

        try {
            bufferBuilder = bufferPool.requestBufferBuilderBlocking();
            return bufferBuilder;
        } catch (InterruptedException e) {
            throw new IOException("Interrupted while waiting for buffer");
        }
    }

    public static class PartitionFileReader implements FileReader<RowData> {

        private final PartitionRecordReader recordReader;
        private final long recordCount;
        private long readerIndex;

        public PartitionFileReader(SubPartition partition,
                                   long recordCount,
                                   TypeSerializer<RowData> inputSerializer,
                                   String tmpDirs) throws IOException {
            this.recordReader = new PartitionRecordReader(partition, new String[] { tmpDirs },
                            inputSerializer);
            this.recordCount = recordCount;
            this.readerIndex = 0;
        }

        @Override
        public boolean hasNext() throws IOException {
            return readerIndex < recordCount;
        }

        @Override
        public RowData next() throws IOException {
            readerIndex++;
            return recordReader.next();
        }

        @Override
        public void close() throws IOException {
            recordReader.clearBuffers();
            recordReader.releaseAllResources();
        }
    }

    public static class PartitionPendingFile
            implements InProgressFileWriter.PendingFile<RowData> {

        private final Path targetPath;
        private final long fileSize;
        private final long rowCount;
        private final SubPartition subPartition;
        private final TypeSerializer<RowData> inputSerializer;
        private final String tmpDir;

        public PartitionPendingFile(
                final Path targetPath,
                final long fileSize,
                final long rowCount,
                final SubPartition subPartition,
                final TypeSerializer<RowData> inputSerializer,
                final String tmpDir) {
            this.targetPath = targetPath;
            this.fileSize = fileSize;
            this.rowCount = rowCount;
            this.subPartition = subPartition;
            this.inputSerializer = inputSerializer;
            this.tmpDir = tmpDir;
        }

        @Override
        public Path getPath() {
            return targetPath;
        }

        @Override
        public long getSize() {
            return fileSize;
        }

        @Override
        public long getRowCount() {
            return rowCount;
        }

        @Override
        public CommittingFile<RowData> commit() {
            return new PartitionCommittingFile(targetPath, fileSize, rowCount, subPartition, inputSerializer, tmpDir);
        }

        @Override
        public void dispose() throws IOException {
            subPartition.release();
        }

        @Override
        public FileReader<RowData> getFileReader() throws IOException {
            return new PartitionFileReader(subPartition, getRowCount(), inputSerializer, tmpDir);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            PartitionPendingFile that =
                    (PartitionPendingFile) o;
            return fileSize == that.fileSize
                    && rowCount == that.rowCount
                    && Objects.equals(targetPath, that.targetPath);
        }

        @Override
        public int hashCode() {
            return Objects.hash(targetPath, fileSize);
        }
    }

    public static class PartitionCommittingFile
            extends PartitionPendingFile implements CommittingFile<RowData> {
        public PartitionCommittingFile(Path targetPath,
                                       long fileSize,
                                       long rowCount,
                                       SubPartition subPartition,
                                       TypeSerializer<RowData> inputSerializer,
                                       String tmpDir) {
            super(targetPath, fileSize, rowCount, subPartition, inputSerializer, tmpDir);
        }
    }

    public static class PartitionCommittingFileSerializer
            implements SimpleVersionedSerializer<CommittingFile<RowData>> {

        private final TypeSerializer<RowData> inputSerializer;
        private static final Charset CHARSET = StandardCharsets.UTF_8;
        private static final int MAGIC_NUMBER = 0x2c853c92;
        private final String tmpDir;
        private final int readBufferSize;

        public PartitionCommittingFileSerializer(TypeSerializer<RowData> inputSerializer,
                                                 String tmpDir,
                                                 int readBufferSize) {
            this.inputSerializer = inputSerializer;
            this.tmpDir = tmpDir;
            this.readBufferSize = readBufferSize;
        }

        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(CommittingFile<RowData> partitionCommittingFile) throws IOException {
            if (!(partitionCommittingFile instanceof PartitionCommittingFile)) {
                throw new UnsupportedOperationException(
                        "Only PartitionCommittingFile is supported.");
            }

            PartitionCommittingFile committingFile =
                    (PartitionCommittingFile) partitionCommittingFile;
            Path path = committingFile.getPath();
            long size = committingFile.getSize();
            long rowCount = committingFile.getRowCount();

            byte[] pathBytes = path.toUri().toString().getBytes(CHARSET);
            byte[] targetBytes = new byte[24 + pathBytes.length];
            ByteBuffer bb = ByteBuffer.wrap(targetBytes).order(ByteOrder.LITTLE_ENDIAN);
            bb.putInt(MAGIC_NUMBER);
            bb.putInt(pathBytes.length);
            bb.put(pathBytes);
            bb.putLong(size);
            bb.putLong(rowCount);

            return targetBytes;
        }

        @Override
        public CommittingFile<RowData> deserialize(int version, byte[] serialized) throws IOException {
            switch (version) {
                case 1:
                    return deserializeV1(serialized);
                default:
                    throw new IOException("Unrecognized version or corrupt state: " + version);
            }
        }

        private PartitionCommittingFile deserializeV1(byte[] serialized)
                throws IOException {
            final ByteBuffer bb = ByteBuffer.wrap(serialized).order(ByteOrder.LITTLE_ENDIAN);

            if (bb.getInt() != MAGIC_NUMBER) {
                throw new IOException("Corrupt data: Unexpected magic number.");
            }

            byte[] targetFilePathBytes = new byte[bb.getInt()];
            bb.get(targetFilePathBytes);
            Path targetFilePath = new Path(new String(targetFilePathBytes, CHARSET));

            long size = bb.getLong();
            long rowCount = bb.getLong();
            File spillFile = new File(targetFilePath.toUri());
            SubPartition partition = SubPartition.createForRead(spillFile, readBufferSize, false);

            return new PartitionCommittingFile(targetFilePath, size, rowCount, partition, inputSerializer, tmpDir);
        }
    }
}
