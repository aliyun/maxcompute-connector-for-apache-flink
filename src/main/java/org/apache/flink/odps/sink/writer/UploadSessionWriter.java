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

package org.apache.flink.odps.sink.writer;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.TunnelBufferedWriter;
import com.aliyun.odps.utils.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.table.OdpsOptions;

/**
 * UploadSessionWriter类实现了OdpsRecordWriter接口
 * 一比一绑定UploadSession和RecordWriter生命周期，用于将数据写入到指定Partition ODPS表中。
 *
 * @author dingxin ()
 */
public class UploadSessionWriter extends OdpsRecordWriter {
    private boolean isClosed;
    private UploadSession uploadSession;
    private RecordWriter recordWriter;
    private long bufferSize;

    public UploadSessionWriter(TableTunnel tunnel, TableIdentifier identifier, String partition, Configuration config)
        throws IOException {
        super(tunnel, identifier, partition, config);
    }

    @Override
    protected void open() throws IOException {
        this.bufferSize = config.get(OdpsOptions.INSERT_WRITER_BUFFER_SIZE).getBytes();
    }

    @Override
    protected void initResource() throws IOException {
        try {
            if (StringUtils.isNullOrEmpty(partition)) {
                this.uploadSession = tunnel.createUploadSession(identifier.getProject(), identifier.getSchema(),
                                                                identifier.getTable(), false);
            } else {
                this.uploadSession = tunnel.createUploadSession(identifier.getProject(), identifier.getSchema(),
                                                                identifier.getTable(), new PartitionSpec(partition),
                                                                false);
            }
            this.recordWriter = uploadSession.openBufferedWriter(compressOption);
            ((TunnelBufferedWriter)this.recordWriter).setBufferSize(bufferSize);
            isClosed = false;
            LOG.info("new upload writer init success, partition: {}, bufferSize: {}, compressOption: {}", partition,
                     bufferSize, compressOption.algorithm.name());
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void recoverResource(String id) throws IOException {
        try {
            if (StringUtils.isNullOrEmpty(partition)) {
                this.uploadSession = tunnel.getUploadSession(identifier.getProject(), identifier.getSchema(), identifier.getTable(), id);
            } else {
                this.uploadSession = tunnel.getUploadSession(identifier.getProject(), identifier.getSchema(), identifier.getTable(),
                                        new PartitionSpec(partition), id);
            }
            this.recordWriter = uploadSession.openBufferedWriter(compressOption);
            ((TunnelBufferedWriter)this.recordWriter).setBufferSize(bufferSize);
            isClosed = false;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public String getId() {
        return uploadSession.getId();
    }

    @Override
    protected void doWrite(Record record) throws IOException {
        recordWriter.write(record);
    }

    @Override
    protected void doFlush() throws IOException {
        ((TunnelBufferedWriter)recordWriter).flush();
    }

    @Override
    public void close() throws IOException {
        if (!isClosed) {
            recordWriter.close();
            isClosed = true;
        }
    }

    @Override
    public void commit() {
        try {
            uploadSession.commit();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 按以下拓扑序尽可能并发执行, 资源受限时效果不佳
     * close writer --> commit session
     * close writer --> new writer
     * new session --> new writer
     */
    public synchronized void reInitializedResource() throws Exception {
        UploadSession oldSession = uploadSession;
        RecordWriter oldWriter = recordWriter;
        boolean closeOldWriter = !isClosed;
        CompletableFuture<Void> closeWriterFuture = CompletableFuture.runAsync(() -> {
            try {
                if (closeOldWriter) {
                    oldWriter.close();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        CompletableFuture<UploadSession> initSessionFuture = CompletableFuture.supplyAsync(() -> {
            try {
                if (StringUtils.isNullOrEmpty(partition)) {
                    return tunnel.createUploadSession(identifier.getProject(), identifier.getSchema(), identifier.getTable(), false);
                }
                return tunnel.createUploadSession(identifier.getProject(), identifier.getSchema(),
                                                  identifier.getTable(), new PartitionSpec(partition), false);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        CompletableFuture<Void> commitSession = closeWriterFuture.thenRunAsync(() -> {
            try {
                oldSession.commit();
            } catch (IOException | TunnelException e) {
                throw new RuntimeException(e);
            }
        });

        this.uploadSession = initSessionFuture.join();
        closeWriterFuture.join();
        this.recordWriter = uploadSession.openBufferedWriter(compressOption);
        ((TunnelBufferedWriter)this.recordWriter).setBufferSize(bufferSize);
        isClosed = false;
        commitSession.join();
        catchSize.set(0L);
    }
}
