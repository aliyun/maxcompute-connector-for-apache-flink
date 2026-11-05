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
 * distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * Modified for the MaxCompute Flink connector; see NOTICE for upstream attribution.
 */

package org.apache.flink.odps.sink.datastream;

import java.io.IOException;

import javax.annotation.Nullable;

import org.apache.flink.odps.shim.FlinkSinkAdapter;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.util.OdpsConf;

import com.aliyun.odps.table.TableIdentifier;
import org.apache.flink.odps.vectorized.ArrowWriterFactory;
import org.apache.flink.table.types.logical.RowType;

/**
 * MaxCompute Sink V2 implementation using Flink Sink V2 API.
 *
 * <p>This sink uses MaxStorage API (replacing Tunnel API) and supports:
 * <ul>
 *   <li>Generic type support - works with RowData, CDC Events, custom POJOs</li>
 *   <li>Automatic dynamic partitioning - no PartitionAssigner needed</li>
 *   <li>At-least-once recovery for realtime append writes; primary-key upserts converge</li>
 *   <li>Record-based writing - simpler than Arrow writer</li>
 * </ul>
 *
 * @param <T> The input data type
 */
public class MaxComputeSink<T> implements FlinkSinkAdapter<T> {

    private static final long serialVersionUID = 1L;

    private final Configuration config;
    private final OdpsConf odpsConf;
    private final TableIdentifier identifier;
    private final String staticPartition;
    private final ArrowWriterFactory<T> arrowWriterFactory;
    private final RowType rowType;

    public MaxComputeSink(
            Configuration config,
            OdpsConf odpsConf,
            TableIdentifier identifier,
            @Nullable String staticPartition,
            ArrowWriterFactory<T> arrowWriterFactory,
            @Nullable RowType rowType) {
        this.config = config;
        this.odpsConf = odpsConf;
        this.identifier = identifier;
        this.staticPartition = staticPartition;
        this.arrowWriterFactory = arrowWriterFactory;
        this.rowType = rowType;
    }

    @Override
    public SinkWriter<T> createStorageWriter(int subtaskId, ProcessingTimeService timers) throws IOException {
        return new MaxComputeSinkWriter<>(
            config, odpsConf, identifier, staticPartition,
            arrowWriterFactory, rowType, subtaskId, timers);
    }
}