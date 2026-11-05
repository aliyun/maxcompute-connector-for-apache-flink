/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * Modified for the MaxCompute Flink connector; see NOTICE for upstream attribution.
 */


package org.apache.flink.odps.sink.table;

import com.aliyun.odps.commons.util.RetryStrategy;
import com.aliyun.odps.table.DataFormat;
import com.aliyun.odps.table.configuration.CompressionCodec;
import com.aliyun.odps.table.configuration.WriterOptions;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.utils.Preconditions;
import org.apache.arrow.memory.BufferAllocator;

public class TunnelWriterOptions extends WriterOptions {

    private WriterOptions options;

    private int numberOfParallel;

    private boolean bufferWriter;

    private long bufferSize;

    private long slotNum;

    public TunnelWriterOptions() {
    }

    public int getNumberOfParallel() {
        return numberOfParallel;
    }

    public long getBufferSize() {
        return bufferSize;
    }

    public boolean isBufferWriter() {
        return bufferWriter;
    }

    public long getSlotNum() {
        return slotNum;
    }

    @Override
    public BufferAllocator getBufferAllocator() {
        return options.getBufferAllocator();
    }

    @Override
    public CompressionCodec getCompressionCodec() {
        return options.getCompressionCodec();
    }

    @Override
    public DataFormat getDataFormat() {
        return options.getDataFormat();
    }

    @Override
    public EnvironmentSettings getSettings() {
        return options.getSettings();
    }

    @Override
    public int getBufferedRowCount() {
        return options.getBufferedRowCount();
    }

    @Override
    public int getChunkSize() {
        return options.getChunkSize();
    }

    @Override
    public RetryStrategy getRetryStrategy() {
        return options.getRetryStrategy();
    }

    public static TunnelWriterOptions.Builder newTunnelWriterOptionsBuilder() {
        return new TunnelWriterOptions.Builder();
    }

    public static class Builder {
        private final WriterOptions.Builder writerOptions = new WriterOptions.Builder();
        private final TunnelWriterOptions tunnelWriterOptions = new TunnelWriterOptions();

        public Builder() {
        }

        public TunnelWriterOptions.Builder withBufferedRowCount(int bufferSize) {
            this.writerOptions.withBufferedRowCount(bufferSize);
            return this;
        }

        public TunnelWriterOptions.Builder withRetryStrategy(RetryStrategy retryStrategy) {
            this.writerOptions.withRetryStrategy(retryStrategy);
            return this;
        }

        public TunnelWriterOptions.Builder withBufferAllocator(BufferAllocator allocator) {
            this.writerOptions.withBufferAllocator(allocator);
            return this;
        }

        public TunnelWriterOptions.Builder withSettings(EnvironmentSettings settings) {
            this.writerOptions.withSettings(settings);
            return this;
        }

        public TunnelWriterOptions.Builder withChunkSize(int chunkSize) {
            Preconditions.checkInteger(chunkSize, 1496, "Chunk size");
            this.writerOptions.withChunkSize(chunkSize);
            return this;
        }

        public TunnelWriterOptions.Builder withDataFormat(DataFormat dataFormat) {
            Preconditions.checkNotNull(dataFormat, "Data format");
            this.writerOptions.withDataFormat(dataFormat);
            return this;
        }

        public TunnelWriterOptions.Builder withCompressionCodec(CompressionCodec codec) {
            this.writerOptions.withCompressionCodec(codec);
            return this;
        }

        public TunnelWriterOptions.Builder withNumberOfParallel(int numberOfParallel) {
            this.tunnelWriterOptions.numberOfParallel = numberOfParallel;
            return this;
        }

        public TunnelWriterOptions.Builder withBufferWriter(boolean bufferWriter) {
            this.tunnelWriterOptions.bufferWriter = bufferWriter;
            return this;
        }

        public TunnelWriterOptions.Builder withBufferSize(long bufferSize) {
            this.tunnelWriterOptions.bufferSize = bufferSize;
            return this;
        }

        public TunnelWriterOptions.Builder withStreamingTunnelSlotNum(long slotNum) {
            this.tunnelWriterOptions.slotNum = slotNum;
            return this;
        }

        public TunnelWriterOptions build() {
            this.tunnelWriterOptions.options = writerOptions.build();
            return this.tunnelWriterOptions;
        }
    }
}
