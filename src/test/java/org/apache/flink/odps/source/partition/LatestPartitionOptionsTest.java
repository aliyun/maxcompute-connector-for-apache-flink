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

import com.aliyun.odps.table.configuration.IncrementalOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.source.OdpsStartupMode;
import org.junit.Test;
import static org.apache.flink.odps.table.OdpsOptions.*;
import static org.junit.Assert.*;

public class LatestPartitionOptionsTest {
    static Configuration config() {
        Configuration c = new Configuration();
        c.set(SCAN_PARTITION_LATEST, true);
        c.set(SCAN_PARTITION_READY_MODE, "partition-visible");
        c.set(SCAN_INCREMENTAL_MODE, IncrementalOptions.IncrementalMode.CDC);
        return c;
    }
    @Test public void defaultRemainsOptOutAndDoesNotRejectLegacyTables() {
        Configuration c = new Configuration(); assertFalse(c.get(SCAN_PARTITION_LATEST));
        LatestPartitionOptions.validate(c, false, false, false, false, true);
    }
    @Test public void acceptsLatestAndExplicitSnapshot() {
        Configuration c = config(); LatestPartitionOptions.validate(c, true, true, true, true, false);
        c.set(SCAN_MODE, OdpsStartupMode.FROM_SNAPSHOT); c.set(SCAN_SNAPSHOT_VERSION, 2L);
        LatestPartitionOptions.validate(c, true, true, true, true, false);
    }
    private void rejects(Configuration c, boolean stream, boolean partitioned, boolean txn, boolean pk, boolean pruned) {
        try { LatestPartitionOptions.validate(c, stream, partitioned, txn, pk, pruned); fail(); }
        catch (IllegalArgumentException expected) { assertFalse(expected.getMessage().isEmpty()); }
    }
    @Test public void rejectsOrdinaryAppendNonPartitionedBatchAndPruning() {
        rejects(config(),true,true,false,false,false);
        rejects(config(),true,true,true,false,false);
        rejects(config(),true,false,true,true,false);
        rejects(config(),false,true,true,true,false);
        rejects(config(),true,true,true,true,true);
        Configuration c = config(); c.set(SCAN_PARTITION,"pt=1"); rejects(c,true,true,true,true,false);
    }
    @Test public void rejectsImplicitReadinessAppendAndUnsupportedStart() {
        Configuration c = config(); c.removeConfig(SCAN_PARTITION_READY_MODE); rejects(c,true,true,true,true,false);
        c = config(); c.set(SCAN_INCREMENTAL_MODE, IncrementalOptions.IncrementalMode.APPEND); rejects(c,true,true,true,true,false);
        c = config(); c.set(SCAN_MODE, OdpsStartupMode.LATEST_FULL); rejects(c,true,true,true,true,false);
        c = config(); c.set(SCAN_MODE, OdpsStartupMode.FROM_SNAPSHOT); rejects(c,true,true,true,true,false);
        c.set(SCAN_SNAPSHOT_VERSION, -1L); rejects(c,true,true,true,true,false);
    }
}
