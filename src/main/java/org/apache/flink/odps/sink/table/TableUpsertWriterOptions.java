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

package org.apache.flink.odps.sink.table;

import java.util.List;

public class TableUpsertWriterOptions {

    private long slotBufferSize = 64 * 1024 * 1024 ;

    private long maxBufferSize = 1024 * 1024;

    private int maxRetries = 3;

    private boolean partialWrite = false;

    private List<String> partialWriteColumns = null;

    private String compressAlgorithm;

    public TableUpsertWriterOptions() {
    }

    public long getMaxBufferSize() {
        return maxBufferSize;
    }

    public long getSlotBufferSize() {
        return slotBufferSize;
    }

    public boolean supportPartialWrite() {
        return partialWrite;
    }

    public List<String> getPartialWriteColumns() {
        return partialWriteColumns;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public String getCompressAlgorithm() {
        return compressAlgorithm;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final TableUpsertWriterOptions tunnelWriterOptions = new TableUpsertWriterOptions();

        public Builder() {
        }

        public TableUpsertWriterOptions.Builder withSlotBufferSize(long slotBufferSize) {
            this.tunnelWriterOptions.slotBufferSize = slotBufferSize;
            return this;
        }

        public TableUpsertWriterOptions.Builder withMaxBufferSize(long maxBufferSize) {
            this.tunnelWriterOptions.maxBufferSize = maxBufferSize;
            return this;
        }

        public TableUpsertWriterOptions.Builder withMaxRetries(int maxRetries) {
            this.tunnelWriterOptions.maxRetries = maxRetries;
            return this;
        }

        public TableUpsertWriterOptions.Builder enablePartialWrite(boolean partialWrite) {
            this.tunnelWriterOptions.partialWrite = partialWrite;
            return this;
        }

        public TableUpsertWriterOptions.Builder withPartialWriteColumns(List<String> columns) {
            this.tunnelWriterOptions.partialWriteColumns = columns;
            return this;
        }

        public TableUpsertWriterOptions.Builder withCompressAlgorithm(String compressAlgorithm) {
            this.tunnelWriterOptions.compressAlgorithm = compressAlgorithm;
            return this;
        }

        public TableUpsertWriterOptions build() {
            return this.tunnelWriterOptions;
        }
    }

}