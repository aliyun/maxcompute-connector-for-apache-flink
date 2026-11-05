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

package org.apache.flink.odps.source.partition;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.odps.source.OdpsStartupMode;
import com.aliyun.odps.table.configuration.IncrementalOptions;
import static org.apache.flink.odps.table.OdpsOptions.*;

/** Validate opt-in semantics before the legacy source can silently fall back to a bounded read. */
public final class LatestPartitionOptions {
    private LatestPartitionOptions() {}

    public static void validate(ReadableConfig conf, boolean streaming, boolean partitioned,
            boolean transactional, boolean primaryKey, boolean pruned) {
        if (!conf.get(SCAN_PARTITION_LATEST)) { return; }
        if (!streaming || !partitioned || !transactional || !primaryKey) {
            throw new IllegalArgumentException("scan.partition.latest requires a partitioned PK Delta table "
                    + "(or PK DLMV) in streaming mode; ordinary and Append Delta tables are unsupported");
        }
        if (conf.getOptional(SCAN_PARTITION).isPresent() || pruned) {
            throw new IllegalArgumentException("scan.partition.latest cannot use a fixed/pruned partition list");
        }
        if (conf.get(SCAN_INCREMENTAL_MODE) != IncrementalOptions.IncrementalMode.CDC) {
            throw new IllegalArgumentException("scan.partition.latest requires scan.incremental.mode=cdc");
        }
        OdpsStartupMode mode = conf.get(SCAN_MODE);
        if (mode != OdpsStartupMode.LATEST && mode != OdpsStartupMode.FROM_SNAPSHOT) {
            throw new IllegalArgumentException("scan.partition.latest supports latest and from-snapshot only");
        }
        if (mode == OdpsStartupMode.FROM_SNAPSHOT
                && (!conf.getOptional(SCAN_SNAPSHOT_VERSION).isPresent()
                    || conf.get(SCAN_SNAPSHOT_VERSION) < 0)) {
            throw new IllegalArgumentException("from-snapshot requires a non-negative scan.snapshot-version");
        }
        if (!"partition-visible".equals(conf.getOptional(SCAN_PARTITION_READY_MODE).orElse(""))) {
            throw new IllegalArgumentException("Set scan.partition.ready-mode=partition-visible only when "
                    + "the producer publishes partitions after committing/refreshing their data");
        }
    }
}
