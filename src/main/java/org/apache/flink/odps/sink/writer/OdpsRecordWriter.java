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
import java.util.concurrent.atomic.AtomicLong;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.io.CompressOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.table.OdpsOptions;
import org.apache.flink.odps.util.OdpsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OdpsRecordWriter 是一个抽象类，用于实现数据到阿里云ODPS表中的写入。
 *
 * @author dingxin ()
 */
public abstract class OdpsRecordWriter implements DataWriter<Record> {

    // 成员变量
    protected static final Logger LOG = LoggerFactory.getLogger(OdpsRecordWriter.class);
    protected final Configuration config;

    /**
     * 表隧道服务实例
     */
    protected final TableTunnel tunnel;
    /**
     * 表标识符
     */
    protected final TableIdentifier identifier;
    /**
     * 分区名称
     */
    protected final String partition;
    /**
     * /**
     * 最大重试次数
     */
    protected final int maxRetries;
    /**
     * 重试延迟时间
     */
    protected final long retryDelay;
    /**
     * 压缩选项
     */
    protected final CompressOption compressOption;
    /**
     * 已捕获的数据量计数器
     */
    protected final AtomicLong catchSize;
    /**
     * 是否需要刷新标志
     */
    protected volatile boolean needFlush;

    // 构造方法

    /**
     * 初始化 OdpsRecordWriter 实例
     *
     * @param tunnel     表隧道服务实例
     * @param identifier 表标识符
     * @param partition  分区名称
     * @param config     配置信息
     * @throws IOException IO异常
     */
    protected OdpsRecordWriter(TableTunnel tunnel, TableIdentifier identifier, String partition, Configuration config)
        throws IOException {
        // 初始化成员变量
        this.tunnel = tunnel;
        this.identifier = identifier;
        this.partition = partition;
        this.config = config;
        this.maxRetries = config.getInteger(OdpsOptions.RETRY_MAX_TIMES);
        this.retryDelay = config.getLong(OdpsOptions.RETRY_DELAY_MILLIS);
        this.compressOption = OdpsUtils.compressOptionOf(config.getString(OdpsOptions.SINK_COMPRESS_ALGO));
        this.catchSize = new AtomicLong(0L);
        open();
        // 调用子类初始化资源的方法
        initResource();
    }

    // 抽象方法

    /**
     * 子类初始化成员变量方法
     */
    protected abstract void open() throws IOException;

    /**
     * 子类初始化资源的方法
     *
     * @throws IOException IO异常
     */
    protected abstract void initResource() throws IOException;

    /**
     * 子类恢复资源的方法
     *
     * @throws IOException IO异常
     */
    protected abstract void recoverResource(String id) throws IOException;

    /**
     * 子类释放资源的方法, 默认为调用close和commit方法
     *
     * @throws IOException IO异常
     */
    protected void releaseResource() throws IOException {
        close();
        commit();
    }

    /**
     * 写入单个记录的方法
     *
     * @param record 待写入的记录
     * @throws IOException IO异常
     */
    protected abstract void doWrite(Record record) throws IOException;

    /**
     * 刷新缓冲区的方法
     *
     * @throws IOException IO异常
     */
    protected abstract void doFlush() throws IOException;

    /**
     * 关闭资源, 在事务化，这个方法等价于pre-commit
     */
    public abstract void close() throws IOException;

    /**
     * 提交当前事务
     */
    public abstract void commit();

    // 公共方法

    /**
     * 子类获取当前实例的ID，用于恢复作业
     */
    public abstract String getId();

    /**
     * 添加待写入的元素
     *
     * @param element 待写入的元素
     * @throws IOException IO异常
     */
    @Override
    public void addElement(Record element) throws IOException {
        doWrite(element);
        if (needFlush) {
            doFlush();
        }
        catchSize.incrementAndGet();
    }

    /**
     * 刷新缓冲区（异步）
     *
     * @throws IOException IO异常
     */
    @Override
    public void flush() throws IOException {
        flush(false);
    }

    /**
     * 异步或同步地刷新缓冲区
     *
     * @param async 是否异步刷新
     * @throws IOException IO异常
     */
    public void flush(boolean async) throws IOException {
        if (async) {
            needFlush = true;
            return;
        }
        doFlush();
    }

    /**
     * 重置当前状态
     */
    @Override
    public void reset() {
        try {
            releaseResource();
            initResource();
            catchSize.set(0L);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void recover(String id) throws IOException {
        recoverResource(id);
    }

    /**
     * 完成写入并关闭资源
     *
     * @param checkpointId 检查点ID
     * @return 提交信息
     * @throws IOException IO异常
     */
    @Override
    public CommitInfo finish(long checkpointId) throws IOException {
        try {
            releaseResource();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    /**
     * 中断当前操作并重新开始
     */
    @Override
    public void abort() {
        try {
            initResource();
            catchSize.set(0L);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // Object 方法

    @Override
    public boolean equals(Object o) {
        if (this == o) {return true;}
        if (o == null || getClass() != o.getClass()) {return false;}

        OdpsRecordWriter that = (OdpsRecordWriter)o;

        if (!identifier.equals(that.identifier)) {return false;}
        return partition.equals(that.partition);
    }

    @Override
    public int hashCode() {
        int result = identifier.hashCode();
        result = 31 * result + partition.hashCode();
        return result;
    }
}

