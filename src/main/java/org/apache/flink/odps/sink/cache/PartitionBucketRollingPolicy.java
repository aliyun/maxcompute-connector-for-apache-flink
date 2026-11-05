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

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;

public class PartitionBucketRollingPolicy extends CheckpointRollingPolicy<RowData, String> {

    private static final long DEFAULT_INACTIVITY_INTERVAL = 600L * 1000L;

    private static final long DEFAULT_ROLLOVER_INTERVAL = 1200L * 1000L;

    private static final long DEFAULT_MAX_PART_SIZE = 1024L * 1024L * 8;

    private final long rollingFileSize;
    private final long rollingTimeInterval;
    private final long inactivityInterval;

    private PartitionBucketRollingPolicy(
            long rollingFileSize, long rollingTimeInterval, long inactivityInterval) {
        Preconditions.checkArgument(rollingFileSize > 0L);
        Preconditions.checkArgument(rollingTimeInterval > 0L);
        Preconditions.checkArgument(inactivityInterval > 0L);
        this.rollingFileSize = rollingFileSize;
        this.rollingTimeInterval = rollingTimeInterval;
        this.inactivityInterval = inactivityInterval;
    }

    @Override
    public boolean shouldRollOnCheckpoint(PartFileInfo<String> partFileState) {
        return true;
    }

    @Override
    public boolean shouldRollOnEvent(PartFileInfo<String> partFileState, RowData element) {
        return false;
    }

    @Override
    public boolean shouldRollOnProcessingTime(
            PartFileInfo<String> partFileState, long currentTime) {
        try {
            return partFileState.getSize() > rollingFileSize;
            // TODO: remove inactive time
            //        || currentTime - partFileState.getCreationTime() >= rollingTimeInterval
            //        || currentTime - partFileState.getLastUpdateTime() >= inactivityInterval;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Creates a new {@link DefaultRollingPolicy.PolicyBuilder} that is used to configure and build an instance of {@code
     * DefaultRollingPolicy}.
     */
    public static PartitionBucketRollingPolicy.PolicyBuilder builder() {
        return new PartitionBucketRollingPolicy.PolicyBuilder(
                DEFAULT_MAX_PART_SIZE, DEFAULT_ROLLOVER_INTERVAL, DEFAULT_INACTIVITY_INTERVAL);
    }

    public static final class PolicyBuilder {

        private final long partSize;

        private final long rolloverInterval;

        private final long inactivityInterval;

        private PolicyBuilder(
                final long partSize, final long rolloverInterval, final long inactivityInterval) {
            this.partSize = partSize;
            this.rolloverInterval = rolloverInterval;
            this.inactivityInterval = inactivityInterval;
        }

        /**
         * Sets the part size above which a part file will have to roll.
         *
         * @param size the allowed part size.
         */
        public PartitionBucketRollingPolicy.PolicyBuilder withMaxPartSize(final MemorySize size) {
            Preconditions.checkNotNull(size, "Rolling policy memory size cannot be null");
            return new PartitionBucketRollingPolicy.PolicyBuilder(size.getBytes(), rolloverInterval, inactivityInterval);
        }

        public PartitionBucketRollingPolicy.PolicyBuilder withInactivityInterval(final Duration interval) {
            Preconditions.checkNotNull(
                    interval, "Rolling policy inactivity interval cannot be null");
            return new PartitionBucketRollingPolicy.PolicyBuilder(partSize, rolloverInterval, interval.toMillis());
        }

        public PartitionBucketRollingPolicy.PolicyBuilder withRolloverInterval(final Duration interval) {
            Preconditions.checkNotNull(interval, "Rolling policy rollover interval cannot be null");
            return new PartitionBucketRollingPolicy.PolicyBuilder(partSize, interval.toMillis(), inactivityInterval);
        }

        public PartitionBucketRollingPolicy build() {
            return new PartitionBucketRollingPolicy(partSize, rolloverInterval, inactivityInterval);
        }
    }
}
