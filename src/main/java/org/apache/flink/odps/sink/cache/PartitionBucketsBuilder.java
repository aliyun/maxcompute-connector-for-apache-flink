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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.filesystem.*;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;

public class PartitionBucketsBuilder implements Serializable {

    private static final long serialVersionUID = 1L;

    private Configuration configuration;

    private BucketAssigner<RowData, String> bucketAssigner;

    private RollingPolicy<RowData, String> rollingPolicy;

    private BucketWriter<RowData, String> bucketWriter;

    private ProcessingTimeService procTimeService;

    private Comparator<String> bucketComparator;

    public PartitionBucketsBuilder(Configuration configuration) {
        this.configuration = Preconditions.checkNotNull(configuration);
    }

    public PartitionBucketsBuilder withBucketComparator(final Comparator<String> bucketComparator) {
        this.bucketComparator = bucketComparator;
        return this;
    }

    public PartitionBucketsBuilder withBucketWriter(final BucketWriter<RowData, String> bucketWriter) {
        this.bucketWriter = bucketWriter;
        return this;
    }

    public PartitionBucketsBuilder withBucketAssigner(final BucketAssigner<RowData, String> assigner) {
        this.bucketAssigner = Preconditions.checkNotNull(assigner);
        return this;
    }

    public PartitionBucketsBuilder withRollingPolicy(final RollingPolicy<RowData, String> policy) {
        this.rollingPolicy = Preconditions.checkNotNull(policy);
        return this;
    }

    public PartitionBucketsBuilder withProcessingTimeService(final ProcessingTimeService procTimeService) {
        this.procTimeService = procTimeService;
        return this;
    }

    public Buckets<RowData, String> createBuckets(int subtaskIndex) throws IOException {
        return new Buckets<>(
                configuration,
                bucketWriter,
                bucketAssigner,
                rollingPolicy,
                subtaskIndex,
                procTimeService,
                bucketComparator);
    }
}
