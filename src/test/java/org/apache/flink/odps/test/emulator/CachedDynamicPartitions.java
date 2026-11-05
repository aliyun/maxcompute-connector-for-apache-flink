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
 */

package org.apache.flink.odps.test.emulator;

import com.aliyun.odps.Odps;
import com.aliyun.odps.Partition;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;

import org.junit.Assert;

import java.util.*;

/** Independent partition/key/value oracle shared by SQL pressure, fault and restore journeys. */
final class CachedDynamicPartitions {
    private CachedDynamicPartitions() {}

    static String sqlColumns() {
        return "CASE MOD(id,8) WHEN 0 THEN CAST(NULL AS STRING) WHEN 1 THEN '' "
                + "WHEN 2 THEN '中文😀' ELSE CONCAT('p',CAST(MOD(id,8) AS STRING)) END AS ds,"
                + "CONCAT('r',CAST(MOD(id,3) AS STRING)) AS region";
    }

    private static String ds(long id) {
        long index = id % 8;
        return index < 2 ? "missing" : index == 2 ? "中文😀" : "p" + index;
    }

    private static String identity(String ds, String region) {
        return ds + "/" + region;
    }

    static int partitionCount(int records) {
        Set<String> partitions = new HashSet<>();
        for (int id = 0; id < records; id++) partitions.add(identity(ds(id), "r" + id % 3));
        return partitions.size();
    }

    static long verify(
            Odps odps,
            TableTunnel tunnel,
            String table,
            int records,
            boolean rewrite,
            boolean noDuplicates)
            throws Exception {
        Map<String, Set<Long>> expected = new TreeMap<>();
        for (long id = 0; id < records; id++)
            expected.computeIfAbsent(identity(ds(id), "r" + id % 3), key -> new HashSet<>())
                    .add(id);
        Map<String, Set<Long>> actual = new TreeMap<>();
        long count = 0;
        for (Partition partition : odps.tables().get("test_project", table).getPartitions()) {
            PartitionSpec spec = partition.getPartitionSpec();
            String name = identity(spec.get("ds"), spec.get("region"));
            Assert.assertTrue("unexpected partition " + spec, expected.containsKey(name));
            Assert.assertFalse("duplicate partition " + spec, actual.containsKey(name));
            Set<Long> ids = new HashSet<>();
            TableTunnel.DownloadSession session =
                    tunnel.createDownloadSession("test_project", table, spec);
            long partitionCount = session.getRecordCount();
            if (partitionCount > 0)
                try (TunnelRecordReader reader = session.openRecordReader(0, partitionCount)) {
                    com.aliyun.odps.data.Record row;
                    while ((row = reader.read()) != null) {
                        long id = row.getBigint(0);
                        Assert.assertTrue(
                                "key routed to wrong partition " + id + " -> " + spec,
                                expected.get(name).contains(id));
                        Assert.assertEquals(
                                id * 7 + (rewrite ? 4 : 3), row.getBigint(1).longValue());
                        if (noDuplicates) Assert.assertTrue("duplicate key " + id, ids.add(id));
                        else ids.add(id);
                    }
                }
            Assert.assertTrue(
                    "unbounded partition replay " + spec,
                    partitionCount >= expected.get(name).size()
                            && partitionCount
                                    <= expected.get(name).size() * (noDuplicates ? 1L : 4L));
            count += partitionCount;
            actual.put(name, ids);
        }
        Assert.assertEquals("partition/key set mismatch", expected, actual);
        return count;
    }
}
