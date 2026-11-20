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

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.table.SessionStatus;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/** Creates session IDs for a coordinator, which owns the shared I/O threads. */
public final class UpsertSessionCreator implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(UpsertSessionCreator.class);

    private final TableIdentifier table;
    private final EnvironmentSettings settings;
    private final EventLoopGroup eventLoops =
            new NioEventLoopGroup(1, new DefaultThreadFactory("odps-upsert-session", true));

    public UpsertSessionCreator(TableIdentifier table, EnvironmentSettings settings) {
        this.table = table;
        this.settings = settings;
    }

    public String create(String partition) throws IOException {
        PartitionSpec spec = StringUtils.isNullOrWhitespaceOnly(partition)
                ? null : new PartitionSpec(partition);
        TableUpsertSessionImpl session = new TableUpsertSessionImpl(table, spec, settings, eventLoops);
        try {
            SessionStatus status = session.getStatus();
            String id = session.getId();
            LOG.info("Create upsert session: {}, {}, {}, {}", id, status, table, partition);
            if (status != SessionStatus.NORMAL) {
                throw new IOException("Invalid session status: " + status + ", session id: " + id);
            }
            return id;
        } finally {
            session.close();
        }
    }

    @Override
    public void close() {
        // These sessions return IDs without opening an Upsert Writer.
        eventLoops.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
    }
}
