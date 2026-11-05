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

import com.aliyun.odps.data.Record;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.impl.UpsertRecord;
import com.aliyun.odps.tunnel.impl.UpsertSessionImpl;
import com.aliyun.odps.tunnel.impl.UpsertSessionImpl.Builder;
import com.aliyun.odps.tunnel.streams.UpsertStream;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.sink.table.BoundedSdkCall;
import org.apache.flink.odps.table.OdpsOptions;
import org.apache.flink.util.StringUtils;

import java.io.IOException;

/**
 * UpsertSessionWriter类实现了OdpsRecordWriter接口
 * 一比一绑定了UpsertSession和UpsertStream的生命周期，用于将数据写入到指定Partition ODPS表中。
 *
 * @author dingxin ()
 */
public class UpsertSessionWriter extends OdpsRecordWriter {
    // Upsert会话实例
    private UpsertSessionImpl upsertSession;
    // Upsert流记录写入器
    private UpsertStream recordWriter;
    // 标记是否已关闭
    protected boolean isClosed;
    private long operationTimeoutMillis;
    private long bufferSize;
    private long slotbufferSize;

    /**
     * 构造方法，初始化成员变量并设置相应的参数。
     *
     * @param tunnel 表通道实例
     * @param identifier 数据表标识符
     * @param partition 分区名称
     * @param config 配置信息
     * @throws IOException 如果初始化资源时发生异常，则抛出IOException
     */
    public UpsertSessionWriter(
            TableTunnel tunnel, TableIdentifier identifier, String partition, Configuration config)
            throws IOException {
        super(tunnel, identifier, partition, config);
        this.isClosed = false;
    }

    @Override
    protected void open() throws IOException {
        bufferSize = config.get(OdpsOptions.UPSERT_WRITER_MAX_BUFFER_SIZE).getBytes();
        slotbufferSize = config.get(OdpsOptions.UPSERT_WRITER_BUCKET_BUFFER_SIZE).getBytes();
    }

    /**
     * 初始化资源，包括创建 UpsertSession 实例和 UpsertStream 实例。
     *
     * @throws IOException 如果初始化资源时发生异常，则抛出IOException
     */
    @Override
    protected void initResource() throws IOException {
        createUpsertSession(null);
        LOG.info(
                "new upsert writer init success, partition: {}, bufferSize: {}, slotBufferSize: {},"
                        + " compressOption: {}",
                partition,
                bufferSize,
                slotbufferSize,
                compressOption.algorithm.name());
    }

    @Override
    protected void recoverResource(String id) throws IOException {
        createUpsertSession(id);
    }

    private void createUpsertSession(String recoverId) throws IOException {
        try {
            // 创建 UpsertSession 实例
            Builder sessionBuilder =
                    new Builder()
                            .setConfig(tunnel.getConfig())
                            .setProjectName(identifier.getProject())
                            .setSchemaName(identifier.getSchema())
                            .setTableName(identifier.getTable());
            // 设置分区
            if (!StringUtils.isNullOrWhitespaceOnly(partition)) {
                sessionBuilder.setPartitionSpec(partition);
            }
            // recover
            if (!StringUtils.isNullOrWhitespaceOnly(recoverId)) {
                sessionBuilder.setUpsertId(recoverId);
            }
            sessionBuilder.setReadTimeout(
                    Math.max(1L, config.get(OdpsOptions.READ_TIMEOUT).toMillis()));
            upsertSession = sessionBuilder.build();
            operationTimeoutMillis = Math.multiplyExact(upsertSession.getReadTimeout(), 4L);

            // 创建 UpsertStream 实例
            this.recordWriter =
                    upsertSession
                            .buildUpsertStream()
                            .setCompressOption(compressOption)
                            .setMaxBufferSize(bufferSize)
                            .setSlotBufferSize(slotbufferSize)
                            .build();

            isClosed = false;
        } catch (TunnelException e) {
            throw new IOException(e);
        }
    }

    /**
     * 写入一行数据。 保证element为UpsertRecord类型
     *
     * @param element 待写入的数据行
     * @throws IOException 如果写入数据时发生异常，则抛出IOException
     */
    @Override
    public void doWrite(Record element) throws IOException {
        invoke(() -> recordWriter.upsert((UpsertRecord) element));
    }

    @Override
    protected void doFlush() throws IOException {
        invoke(recordWriter::flush);
    }

    @Override
    public void close() throws IOException {
        if (!isClosed) {
            try {
                invoke(recordWriter::close);
            } finally {
                isClosed = true;
                upsertSession.close();
            }
        }
    }

    private void invoke(BoundedSdkCall.Action action) throws IOException {
        try {
            BoundedSdkCall.run(operationTimeoutMillis, action);
        } catch (IOException failure) {
            isClosed = true;
            upsertSession.close();
            throw failure;
        }
    }

    @Override
    public void commit() {
        try {
            upsertSession.commit(false);
        } catch (TunnelException e) {
            throw new RuntimeException(e);
        } finally {
            upsertSession.close();
        }
    }

    @Override
    public String getId() {
        return upsertSession.getId();
    }
}
