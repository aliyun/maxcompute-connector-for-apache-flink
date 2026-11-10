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

package org.apache.flink.odps.sink.event;

import org.apache.flink.odps.sink.utils.WriterStatus;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SinkTaskEvent implements OperatorEvent {

    private final Integer taskID;
    private final Long checkpointID;

    private List<WriterStatus> writerStatuses;
    private String partitionSpec;
    private boolean endInput;
    private boolean bootstrap;

    private boolean writeSuccess;

    private boolean checkpointSuccess;
    // Only an ended cached writer whose rows have all committed may set this flag.
    private boolean inputCommitted;

    private SinkTaskEvent(
            int taskID,
            Long checkpointID,
            List<WriterStatus> writerStatuses,
            String partitionSpec,
            boolean bootstrap,
            boolean endInput,
            boolean writeSuccess,
            boolean checkpointSuccess,
            boolean inputCommitted) {
        this.taskID = taskID;
        this.checkpointID = checkpointID;
        this.writerStatuses = new ArrayList<>(writerStatuses);
        this.partitionSpec = partitionSpec;
        this.bootstrap = bootstrap;
        this.endInput = endInput;
        this.writeSuccess = writeSuccess;
        this.checkpointSuccess = checkpointSuccess;
        this.inputCommitted = inputCommitted;
    }

    public Integer getTaskID() {
        return taskID;
    }

    public List<WriterStatus> getWriterStatuses() {
        return writerStatuses;
    }

    public boolean isBootstrap() {
        return bootstrap;
    }

    public boolean isEndInput() {
        return endInput;
    }

    public boolean isWriteSuccess() {
        return writeSuccess;
    }

    public Long getCheckpointID() {
        return checkpointID;
    }

    public String getPartitionSpec() {
        return partitionSpec;
    }

    public static Builder builder() {
        return new Builder();
    }

    public boolean isCheckpointSuccess() {
        return checkpointSuccess;
    }

    public boolean isInputCommitted() {
        return inputCommitted;
    }

    @Override
    public String toString() {
        return "SinkTaskEvent{"
                + "taskID=" + taskID
                + ", partition=" + partitionSpec
                + ", endInput=" + endInput
                + ", bootstrap=" + bootstrap
                + ", checkpointSuccess=" + checkpointSuccess
                + ", inputCommitted=" + inputCommitted
                + ", writeSuccess=" + writeSuccess
                + '}';
    }

    /**
     * Builder for {@link SinkTaskEvent}.
     */
    public static class Builder {
        private Integer taskID;
        private List<WriterStatus> writerStatuses = new ArrayList<>();
        private boolean endInput = false;
        private boolean bootstrap = false;
        private boolean checkpointSuccess = false;
        private boolean inputCommitted = false;
        private boolean writeSuccess = true;
        private String partitionSpec = "";
        private Long checkpointID = -1L;

        public SinkTaskEvent build() {
            Objects.requireNonNull(taskID);
            Objects.requireNonNull(writerStatuses);
            return new SinkTaskEvent(taskID, checkpointID, writerStatuses, partitionSpec,
                    bootstrap, endInput, writeSuccess, checkpointSuccess, inputCommitted);
        }

        public Builder taskID(int taskID) {
            this.taskID = taskID;
            return this;
        }

        public Builder checkpointID(long checkpointID) {
            this.checkpointID = checkpointID;
            return this;
        }

        public Builder writeStatus(List<WriterStatus> writerStatuses) {
            this.writerStatuses = writerStatuses;
            return this;
        }

        public Builder writeSuccess(boolean writeSuccess) {
            this.writeSuccess = writeSuccess;
            return this;
        }

        public Builder requiredPartition(String partitionSpec) {
            this.partitionSpec = partitionSpec;
            return this;
        }

        public Builder endInput(boolean endInput) {
            this.endInput = endInput;
            return this;
        }

        public Builder bootstrap(boolean bootstrap) {
            this.bootstrap = bootstrap;
            return this;
        }

        public Builder checkpointSuccess(boolean checkpointSuccess) {
            this.checkpointSuccess = checkpointSuccess;
            return this;
        }

        public Builder inputCommitted(boolean inputCommitted) {
            this.inputCommitted = inputCommitted;
            return this;
        }
    }
}
