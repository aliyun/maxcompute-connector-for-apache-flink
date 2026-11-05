/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * Modified for the MaxCompute Flink connector; see NOTICE for upstream attribution.
 */

package org.apache.flink.odps.sink.event;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;

public class TaskAckEvent implements OperatorEvent {
    private static final long serialVersionUID = 1L;

    private long checkpointId;

    private boolean committed;

    private boolean endInput;

    private boolean bootstrap;

    private boolean checkpoint;

    private String partition;

    private String sessionId;

    public TaskAckEvent() {
    }

    public TaskAckEvent(long checkpointId,
                        boolean committed,
                        boolean endInput,
                        boolean bootstrap,
                        boolean checkpoint,
                        String partition,
                        String sessionId) {
        this.checkpointId = checkpointId;
        this.committed = committed;
        this.endInput = endInput;
        this.bootstrap = bootstrap;
        this.checkpoint = checkpoint;
        this.partition = partition;
        this.sessionId = sessionId;
    }

    public String getPartition() {
        return partition;
    }

    public String getSessionId() {
        return sessionId;
    }

    public boolean isCommitted() {
        return committed;
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    public boolean isEndInput() {
        return endInput;
    }

    public boolean isBootstrap() {
        return bootstrap;
    }

    public boolean isCheckpoint() {
        return checkpoint;
    }

    @Override
    public String toString() {
        return "TaskAckEvent{"
                + "committed=" + committed
                + ", checkpoint=" + checkpoint
                + ", bootstrap=" + bootstrap
                + ", endInput=" + endInput
                + ", partition=" + partition
                + ", sessionId=" + sessionId
                + ", checkpointId=" + checkpointId
                + '}';
    }

    public static TaskAckEvent.Builder builder() {
        return new TaskAckEvent.Builder();
    }

    public static class Builder {
        private long checkpointId;
        private String partition;
        private String sessionId;
        private boolean committed;
        private boolean endInput;
        private boolean bootstrap;
        private boolean checkpoint;

        public TaskAckEvent.Builder checkpointId(long checkpointId) {
            this.checkpointId = checkpointId;
            return this;
        }

        public TaskAckEvent.Builder committed(boolean committed) {
            this.committed = committed;
            return this;
        }

        public TaskAckEvent.Builder endInput(boolean endInput) {
            this.endInput = endInput;
            return this;
        }

        public TaskAckEvent.Builder bootstrap(boolean bootstrap) {
            this.bootstrap = bootstrap;
            return this;
        }

        public TaskAckEvent.Builder checkpoint(boolean checkpoint) {
            this.checkpoint = checkpoint;
            return this;
        }

        public TaskAckEvent.Builder sessionId(String sessionId) {
            this.sessionId = sessionId;
            return this;
        }

        public TaskAckEvent.Builder partition(String partition) {
            this.partition = partition;
            return this;
        }

        public TaskAckEvent build() {
            return new TaskAckEvent(checkpointId, committed, endInput, bootstrap, checkpoint, partition, sessionId);
        }
    }

}
