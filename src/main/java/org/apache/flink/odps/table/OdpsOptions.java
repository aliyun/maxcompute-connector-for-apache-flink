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

package org.apache.flink.odps.table;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.aliyun.odps.table.configuration.CompressionCodec;
import com.aliyun.odps.table.configuration.IncrementalOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.odps.source.OdpsSplitStrategy;
import org.apache.flink.odps.source.OdpsStartupMode;
import org.apache.flink.odps.util.Constants;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.odps.util.Constants.DEFAULT_ODPS_META_CACHE_EXPIRE_TIME;
import static org.apache.flink.odps.util.Constants.DEFAULT_ODPS_META_CACHE_SIZE;

public class OdpsOptions {

    public static final ConfigOption<String> PROJECT_NAME =
            ConfigOptions.key(Constants.ODPS_PROJECT_NAME)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The name of Odps project to connect.");

    public static final ConfigOption<String> ACCESS_ID =
            ConfigOptions.key(Constants.ODPS_ACCESS_ID)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The access id of Odps project");

    public static final ConfigOption<String> ACCESS_KEY =
            ConfigOptions.key(Constants.ODPS_ACCESS_KEY)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The access key of Odps project");

    public static final ConfigOption<String> END_POINT =
            ConfigOptions.key(Constants.ODPS_END_POINT)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The end point of Odps project");

    public static final ConfigOption<String> REGION_ID =
        ConfigOptions.key("odps.region.id")
            .stringType()
            .noDefaultValue()
            .withDescription("The region id of odps project, if set this, use v4 signature");

    public static final ConfigOption<String> SIGNATURE_CORPORATION =
        ConfigOptions.key("odps.signature.corporation")
            .stringType()
            .defaultValue("aliyun")
            .withDescription("Only v4 signature used, in idpt scenarios, we need to modify this to 'apsara'");

    public static final ConfigOption<String> CREDENTIALS_PROVIDER =
        ConfigOptions.key(Constants.ODPS_CREDENTIALS_PROVIDER)
            .stringType()
            .noDefaultValue()
            .withDescription("The fully qualified class name or identifier of the credential provider used for ODPS authentication.");

    public static final ConfigOption<Map<String, String>> CREDENTIALS_PROVIDER_PARAM =
        ConfigOptions.key(Constants.ODPS_CREDENTIALS_PROVIDER_PARAMS)
            .mapType()
            .noDefaultValue()
            .withDescription("Parameters as key-value pairs used to construct the credential provider instance, e.g., access keys or custom properties.");

    public static final ConfigOption<String> HTTP_PROXY =
            ConfigOptions.key(Constants.ODPS_HTTP_PROXY)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The http proxy url to connect odps");

    public static final ConfigOption<String> HTTPS_PROXY =
            ConfigOptions.key(Constants.ODPS_HTTPS_PROXY)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The https proxy url to connect odps");

    public static final ConfigOption<Boolean> ODPS_PROXY_DISABLE_RESOLVER =
            ConfigOptions.key(Constants.ODPS_PROXY_DISABLE_RESOLVER)
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Disable odps proxy local dns resolver");

    public static final ConfigOption<String> TUNNEL_END_POINT =
            ConfigOptions.key(Constants.ODPS_TUNNEL_END_POINT)
                    .stringType()
                    .defaultValue("")
                    .withDescription("The tunnel end point of Odps project");

    public static final ConfigOption<String> TUNNEL_QUOTA_NAME =
            ConfigOptions.key(Constants.ODPS_TUNNEL_QUOTA_NAME)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Specific quota for tunnel service");

    public static final ConfigOption<Boolean> ODPS_NAMESPACE_SCHEMA_ENABLE =
            ConfigOptions.key("odps.namespace.schema")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Odps namespace schema enable");

    public static final ConfigOption<String> TABLE_PATH =
            ConfigOptions.key(Constants.ODPS_TABLE)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The name of Odps table to connect.");

    public static final ConfigOption<CompressionCodec> COMPRESS_CODEC =
            ConfigOptions.key("compressCodec")
                    .enumType(CompressionCodec.class)
                    .defaultValue(CompressionCodec.NO_COMPRESSION)
                    .withDescription("compress codec name");
    public static final ConfigOption<OdpsSplitStrategy> SPLIT_STRATEGY =
            key("splitStrategy".toLowerCase())
                    .enumType(OdpsSplitStrategy.class)
                    .defaultValue(OdpsSplitStrategy.ROW_OFFSET);

    public static final ConfigOption<Boolean> DYNAMIC_LOAD_BALANCING =
            key("dynamicLoadBalancing".toLowerCase()).booleanType().defaultValue(false);

    public static final ConfigOption<Long> SPLIT_BYTE_SIZE =
            key("splitByteSize".toLowerCase()).longType().defaultValue(256 * 1024L * 1024L);

    public static final ConfigOption<Integer> SPLIT_ROW_OFFSET_GRANULARITY =
            key("splitRowOffsetGranularity".toLowerCase()).intType().defaultValue(1);

    public static final ConfigOption<String> PARTITION =
            key("partition".toLowerCase()).stringType().noDefaultValue();

    public static final ConfigOption<Integer> SPLIT_BATCH_READ_SIZE =
            key("splitBatchReadSize".toLowerCase()).intType().defaultValue(4096);

    public static final ConfigOption<Duration> CONTINUOUS_DISCOVERY_INTERVAL =
            key("continuousDiscoveryInterval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(60))
                    .withDescription("The discovery interval of continuous reading.");

    public static final ConfigOption<Integer> SCAN_MAX_SPLITS_PER_TASK =
            key("maxSplitsPerTask")
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            "Max split size should be cached for one task while scanning. "
                                    + "If splits size cached in enumerator are greater than tasks size multiply by this value, scanner will pause scanning.");

    public static final ConfigOption<Boolean> SCAN_PARTITION_LATEST =
            key("scan.partition.latest").booleanType().defaultValue(false)
                    .withDescription("Follow newly published partitions in order using bounded CDC sessions. "
                            + "Switching abandons unread old splits and subsequent old-partition updates.");

    public static final ConfigOption<String> SCAN_PARTITION_READY_MODE =
            key("scan.partition.ready-mode").stringType().noDefaultValue()
                    .withDescription("partition-visible: producer guarantees a visible partition is ready. "
                            + "Required explicitly for scan.partition.latest.");

    public static final ConfigOption<String> SCAN_PARTITION =
            key("scan.partition")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Static partition spec for source reading, e.g. 'pt=0'. "
                            + "Used as fallback when optimizer partition pruning cannot fire "
                            + "(e.g. WatermarkAssigner blocking the Filter-TableScan pattern).");

    public static final ConfigOption<OdpsStartupMode> SCAN_MODE =
            key("scan.mode")
                    .enumType(OdpsStartupMode.class)
                    .defaultValue(OdpsStartupMode.LATEST)
                    .withDescription("Specify the scanning behavior of the source.");

    public static final ConfigOption<Long> SCAN_SNAPSHOT_VERSION =
            key("scan.snapshot-version")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional snapshot id used in case of \"from-snapshot\" or \"from-snapshot-full\" scan mode");

    public static final ConfigOption<String> SCAN_TIMESTAMP =
            key("scan.timestamp")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional timestamp used in case of \"from-timestamp\" scan mode, supported format：yyyy-MM-dd HH:mm:ss.SSS, use odps.timezone");

    public static final ConfigOption<IncrementalOptions.IncrementalMode> SCAN_INCREMENTAL_MODE =
            key("scan.incremental.mode")
                    .enumType(IncrementalOptions.IncrementalMode.class)
                    .defaultValue(IncrementalOptions.IncrementalMode.APPEND);

    public static final ConfigOption<String> INCREMENTAL_BETWEEN =
            key("scan.incremental-between")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Read incremental changes between start snapshot (exclusive) and end snapshot (inclusive), "
                                    + "for example, '5,10' means changes between snapshot 5 and snapshot 10.");

    public static final ConfigOption<String> INCREMENTAL_BETWEEN_TIMESTAMP =
            key("scan.incremental-between-timestamp")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Read incremental changes between start timestamp (exclusive) and end timestamp (inclusive), "
                                    + "for example, 't1,t2' means changes between timestamp t1 and timestamp t2.");

    public static final ConfigOption<String> SINK_OPERATION =
            ConfigOptions.key("sink.operation")
                    .stringType()
                    .defaultValue("insert")
                    .withDescription("The sink operation, that this write should do.");

    public static final ConfigOption<Integer> SINK_PARALLELISM =
            ConfigOptions.key("sink.parallelism")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines a custom parallelism for the sink. "
                                    + "By default, if this option is not defined, the planner will derive the parallelism "
                                    + "for each statement individually by also considering the global configuration.");

    public static final ConfigOption<String> SINK_PARTITION =
            ConfigOptions.key("sink.partition")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The partition name");

    public static final ConfigOption<Integer> SINK_DYNAMIC_PARTITION_LIMIT =
            ConfigOptions.key("sink.dynamic-partition.limit")
                    .intType()
                    .defaultValue(100)
                    .withDescription("The max limit of dynamic partition");

    public static final ConfigOption<Boolean> SINK_GROUP_PARTITION =
            ConfigOptions.key("sink.group-partition.enable")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Odps group partition enable");

    public static final ConfigOption<Boolean> SINK_FILE_CACHED_ENABLE =
            ConfigOptions.key("sink.file-cached.enable")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Odps file cached sink enable.");

    public static final ConfigOption<Boolean> SINK_ASYNC_COMMIT_ENABLE =
            ConfigOptions.key("sink.commit.async.enable")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Odps file cached sink enable.");

    public static final ConfigOption<String> PARTITION_DEFAULT_VALUE =
            ConfigOptions.key("sink.partition.default-value")
                    .stringType()
                    .defaultValue("__DEFAULT_PARTITION__")
                    .withDescription("The default partition name in case the dynamic partition" +
                            " column value is null/empty string");

    public static final ConfigOption<String> PARTITION_ASSIGNER_CLASS =
            ConfigOptions.key("sink.partition.assigner.class")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The assigner class for implement PartitionAssigner interface.");

    // For upsert
    public static final ConfigOption<Integer> UPSERT_WRITE_BUCKET_NUM = ConfigOptions
            .key("upsert.write.bucket.num")
            .intType()
            .noDefaultValue()
            .withDescription("Odps upsert bucket num.");

    public static final ConfigOption<Integer> UPSERT_WRITE_SLOT_NUM = ConfigOptions
            .key("upsert.write.slot-num")
            .intType()
            .defaultValue(1)
            .withDescription("Odps upsert slot num.");

    public static final ConfigOption<Integer> UPSERT_WRITER_THREAD_NUM = ConfigOptions
            .key("upsert.writer.thread-num")
            .intType()
            .defaultValue(1)
            .withDescription("Odps upsert writer nio thread num.");

    public static final ConfigOption<Integer> UPSERT_WRITER_CONNECTION_LIMIT = ConfigOptions
            .key("upsert.writer.connection.limit")
            .intType()
            .defaultValue(1)
            .withDescription("Odps upsert writer connection limit.");

    public static final ConfigOption<MemorySize> UPSERT_WRITER_BUCKET_BUFFER_SIZE = ConfigOptions
            .key("upsert.writer.bucket.buffer-size")
            .memoryType()
            .defaultValue(MemorySize.parse("1mb"))
            .withDescription("Odps upsert slot buffer size.");

    public static final ConfigOption<MemorySize> UPSERT_WRITER_MAX_BUFFER_SIZE = ConfigOptions
            .key("upsert.writer.buffer-size")
            .memoryType()
            .defaultValue(MemorySize.parse("64mb"))
            .withDescription("Odps upsert writer max buffer size.");

    public static final ConfigOption<Integer> UPSERT_WRITER_FLUSH_MAX_RETRIES = ConfigOptions
            .key("upsert.writer.max-retries")
            .intType()
            .defaultValue(3)
            .withDescription("Odps upsert writer flush max retries.");

    public static final ConfigOption<Integer> UPSERT_COMMIT_THREAD_NUM = ConfigOptions
            .key("upsert.commit.thread-num")
            .intType()
            .defaultValue(16)
            .withDescription("Odps upsert commit thread number.");

    public static final ConfigOption<Integer> UPSERT_COMMIT_MAX_RETRIES = ConfigOptions
            .key("upsert.commit.max-retries")
            .intType()
            .defaultValue(3)
            .withDescription("Odps upsert commit max retries.");

    public static final ConfigOption<Integer> UPSERT_COMMIT_TIMEOUT = ConfigOptions
            .key("upsert.commit.timeout")
            .intType()
            .defaultValue(600)
            .withDescription("Odps upsert commit timeout.");

    public static final ConfigOption<Integer> UPSERT_FLUSH_CONCURRENT = ConfigOptions
            .key("upsert.flush.concurrent")
            .intType()
            .defaultValue(2)
            .withDescription("The maximum concurrency of each task manager's upsert when flushing data. ");

    public static final ConfigOption<Integer> UPSERT_NIO_THREAD_NUM = ConfigOptions
            .key("upsert.nio.thread-num")
            .intType()
            .defaultValue(1)
            .withDescription("The number of thread of netty eventloop to flush data.");

    public static final ConfigOption<Boolean> UPSERT_MAJOR_COMPACT_ENABLE = ConfigOptions
            .key("upsert.major-compact.enable")
            .booleanType()
            .defaultValue(false)
            .withDescription("Odps upsert major compact enable.");

    public static final ConfigOption<Integer> UPSERT_MAJOR_COMPACT_MIN_COMMITS = ConfigOptions
            .key("upsert.major-compact.min-commits")
            .intType()
            .defaultValue(100)
            .withDescription("Odps upsert major compact min commits.");

    public static final ConfigOption<Boolean> UPSERT_PARTIAL_COLUMN_ENABLE = ConfigOptions
            .key("upsert.partial-column.enable")
            .booleanType()
            .defaultValue(false)
            .withDescription("Odps upsert partial column enable.");
    public static final ConfigOption<String> UPSERT_PARTIAL_COLUMN_NAME = ConfigOptions
            .key("upsert.partial-column.name")
            .stringType()
            .noDefaultValue()
            .withDescription("Odps upsert partial column name.");

    // For insert
    public static final ConfigOption<Boolean> INSERT_STREAMING_TUNNEL_ENABLE = ConfigOptions
        .key("insert.streaming-tunnel.enable")
        .booleanType()
        .defaultValue(false)
        .withDescription("Use odps streaming tunnel to write.");

    public static final ConfigOption<Integer> INSERT_STREAMING_TUNNEL_SLOT_NUM = ConfigOptions
        .key("insert.streaming-tunnel.slot-num")
        .intType()
        .defaultValue(0)
        .withDescription("Slot number of odps streaming tunnel");

    public static final ConfigOption<Boolean> INSERT_ARROW_WRITER_ENABLE = ConfigOptions
            .key("insert.arrow-writer.enable")
            .booleanType()
            .defaultValue(false)
            .withDescription("Use odps arrow writer.");

    public static final ConfigOption<Long> INSERT_ARROW_WRITE_BATCH_SIZE = ConfigOptions
            .key("insert.arrow-writer.batch-size")
            .longType()
            .defaultValue(512L)
            .withDescription("Odps arrow writer batch size");

    public static final ConfigOption<MemorySize> INSERT_WRITER_BUFFER_SIZE = ConfigOptions
            .key("insert.writer.buffer-size")
            .memoryType()
            .defaultValue(MemorySize.parse("64mb"))
            .withDescription("Odps insert writer max buffer size.");

    public static final ConfigOption<Integer> INSERT_WRITER_MAX_PENDING_BUFFERS = ConfigOptions
            .key("insert.writer.max-pending-buffers")
            .intType()
            .defaultValue(1)
            .withDescription("Maximum number of buffers that can be in-flight for async upload. "
                + "Higher values absorb network latency spikes but increase memory usage "
                + "(peak memory per subtask = (maxPendingBuffers + 1) * buffer-size).");

    public static final ConfigOption<Long> INSERT_ARROW_WRITER_FLUSH_INTERVAL = ConfigOptions
            .key("insert.arrow-writer.flush-interval")
            .longType()
            .defaultValue(100 * 1000L)
            .withDescription("Writing option, the interval to flush any buffered rows. ");

    public static final ConfigOption<Integer> INSERT_COMMIT_THREAD_NUM = ConfigOptions
            .key("insert.commit.thread-num")
            .intType()
            .defaultValue(16)
            .withDescription("Odps insert commit thread number.");

    public static final ConfigOption<Integer> SINK_ASYNC_FLUSH_THREAD_NUM = ConfigOptions
        .key("sink.async-flush.thread-num")
        .intType()
        .defaultValue(4)
        .withDescription("Sink async flush thread num, current only DynamicStreamingTunnelSink used.");

    // TODO: For file cached options
    public static final ConfigOption<Integer> SINK_THREAD_NUM = ConfigOptions
            .key("sink.file-cached.writer.num")
            .intType()
            .defaultValue(16)
            .withDescription("Odps group sink thread number.");

    public static final ConfigOption<Long> SINK_BUCKET_CHECK_INTERVAL = ConfigOptions
            .key("sink.bucket.check-interval")
            .longType()
            .defaultValue(60 * 1000L)
            .withDescription("The interval to flush any buffered bucket.");

    public static final ConfigOption<Integer> SINK_META_CACHE_SIZE =
            ConfigOptions.key("sink.meta.cache.time")
                    .intType()
                    .defaultValue(DEFAULT_ODPS_META_CACHE_SIZE)
                    .withDescription("");

    public static final ConfigOption<Integer> SINK_META_EXPIRE_TIME =
            ConfigOptions.key("sink.meta.cache.expire.time")
                    .intType()
                    .defaultValue(DEFAULT_ODPS_META_CACHE_EXPIRE_TIME)
                    .withDescription("");

    public static final ConfigOption<MemorySize> SINK_FILE_CACHE_MEMORY =
            key("sink.file-cached.memory")
                    .memoryType()
                    .defaultValue(MemorySize.parse("64m"))
                    .withDescription("");

    public static final ConfigOption<MemorySize> SINK_FILE_MEMORY_SEGMENT_SIZE =
            key("sink.file-cached.memory.segment-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("128kb"))
                    .withDescription("");

    public static final ConfigOption<Integer> SINK_FILE_WRITER_MAX_RETRIES = ConfigOptions
            .key("sink.file-cached.write.max-retries")
            .intType()
            .defaultValue(3)
            .withDescription("Odps file cached writer max retries.");

    public static final ConfigOption<MemorySize> SINK_FILE_ROLLING_MAX_SIZE = ConfigOptions
            .key("sink.file-cached.rolling.max-size")
            .memoryType()
            .defaultValue(MemorySize.parse("16m"))
            .withDescription("");

    public static final ConfigOption<Boolean> SINK_FILE_FLUSH_ALWAYS = ConfigOptions
            .key("sink.file-cached.flush.always")
            .booleanType()
            .defaultValue(true)
            .withDescription("");

    public static final ConfigOption<String> SINK_FILE_TMP_DIRS = ConfigOptions
            .key("sink.file-cached.tmp.dirs")
            .stringType()
            .defaultValue("./local")
            .withDescription("Directories for temporary files");

    public static final ConfigOption<MemorySize> STATE_FILE_THRESHOLD =
            ConfigOptions.key("sink.file-cached.file-threshold")
                    .memoryType()
                    .defaultValue(MemorySize.parse("4m"))
                    .withDescription(
                            "The minimum size of state data files");

    /**
     * 这个配置 与 sink.coordinator.enable 功能相同，不同的是，这个算子不会移除 Coordinator 算子，而是让 TaskManager 不再与 Coordinator 通信
     * 由于复用了 Coordinator，启用该配置后，状态可以与原有状态兼容
     */
    public static final ConfigOption<Boolean> SINK_STANDALONE_ENABLE =
            ConfigOptions.key("sink.standalone.enable")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("enable to remove coordinator. " +
                            "Automatically enabled when insert.streaming-tunnel.enable=true for better performance");

    /**
     * 这个配置决定是否使用一套全新的 Sink 算子（基于 Flink 1.x DataStream API），当配置为 false 时，启用新流程
     * 新流程移除了 Coordinator（Task 之间的信息同步机制），每个 Task 会单独创建 Session，提交 Session，在不考虑 Meta 限流的场景下，性能更好
     * 由于使用了全新的 Sink 算子，启用该配置后，状态无法与原有状态兼容。因此建议使用 sink.standalone.enable 替换该配置
     *
     */
    public static final ConfigOption<Boolean> SINK_COORDINATOR_ENABLE =
        ConfigOptions.key("sink.coordinator.enable")
            .booleanType()
            .defaultValue(true)
            .withDescription("[Deprecated] use 'sink.standalone.enable' instead of this");

    public static final ConfigOption<Integer> RETRY_MAX_TIMES =
            ConfigOptions.key("sink.retry.times")
                    .intType()
                    .defaultValue(3)
                    .withDescription("retry times when doing network IO");

    public static final ConfigOption<Long> RETRY_DELAY_MILLIS =
            ConfigOptions.key("sink.retry.delay")
                    .longType()
                    .defaultValue(2000L)
                    .withDescription("delay time between each retry in milliseconds");

    public static final ConfigOption<String> SINK_COMPRESS_ALGO =
        ConfigOptions.key("sink.compress.algorithm")
            .stringType()
            .defaultValue("zlib")
            .withDescription("compress algorithm name");

    public static final ConfigOption<Long> SINK_IDLE_PARTITION_TIMEOUT =
            ConfigOptions.key("sink.idle-partition.timeout")
                    .longType()
                    .defaultValue(TimeUnit.MINUTES.toMinutes(10))
                    .withDescription("auto release resource of idle partition timeout");

    public static final ConfigOption<Boolean> SCAN_DISK_SPILL_ENABLED =
            ConfigOptions.key("scan.disk-spill.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Enable disk spill buffering for source reads. "
                            + "When enabled, a background thread continuously drains data from the connection "
                            + "and spills to disk when memory buffer is full, preventing connection idle timeouts.");

    public static final ConfigOption<Integer> SCAN_DISK_SPILL_MEMORY_CAPACITY =
            ConfigOptions.key("scan.disk-spill.memory-capacity")
                    .intType()
                    .defaultValue(64)
                    .withDescription("Maximum number of Arrow batches to hold in memory before spilling to disk.");

    public static final ConfigOption<Long> SCAN_DISK_SPILL_MAX_BYTES =
            ConfigOptions.key("scan.disk-spill.max-bytes")
                    .longType()
                    .defaultValue(10L * 1024 * 1024 * 1024)
                    .withDescription("Maximum total bytes that can be spilled to disk.");

    public static final ConfigOption<Duration> SINK_FLUSH_MAX_INTERVAL =
            ConfigOptions.key("sink.flush.timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription("max timeout of sink flush");


    public static final ConfigOption<Duration> READ_TIMEOUT =
        ConfigOptions.key("sink.read.timeout")
            .durationType()
            .defaultValue(Duration.ofSeconds(120))
            .withDescription("max timeout of sink read");

    public static final ConfigOption<Boolean> SINK_DYNAMIC_SHUFFLE_ENABLE =
            ConfigOptions.key("sink.dynamic-shuffle.enable")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "When true, enable traffic-aware dynamic rebalancing for primary-key " +
                            "partitioned tables: a statistics operator collects (partitionSpec, " +
                            "bucketId) record counts, and a shuffle coordinator periodically " +
                            "recomputes a balanced partition→subtask assignment and broadcasts " +
                            "it to the partitioner. When false (default), routing uses a " +
                            "stateless hash of (partitionSpec, bucketId) — still partition+bucket " +
                            "aware, just not rebalanced based on traffic. Has no effect on " +
                            "non-primary-key tables or fully static partition writes. Only applies " +
                            "to the new sink path (sink.coordinator.enable=false).");

    public static final ConfigOption<Boolean> SINK_SHUFFLE_BUCKET_ONLY_ENABLE =
            ConfigOptions.key("sink.shuffle.bucket-only.enable")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "When true, shuffle by bucketId only (ignoring partition spec), " +
                            "for performance A/B comparison. This does NOT guarantee PK " +
                            "correctness on partitioned tables — same bucketId across " +
                            "different partitions may land on different subtasks. Only " +
                            "applies to the new sink path (sink.coordinator.enable=false) " +
                            "on primary-key tables.");
}
