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

package org.apache.flink.odps.sink.common;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractWriteFunction<I> extends ProcessFunction<I, Object> implements BoundedOneInput {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractWriteFunction.class);

    protected final Configuration config;

    protected transient StreamTask<?, ?> container;

    protected transient StreamConfig streamConfig;

    protected transient OperatorEventGateway eventGateway;

    protected transient ProcessingTimeService processingTimeService;

    protected transient boolean inputEnded;

    protected transient MailboxExecutor executor;

    public AbstractWriteFunction(Configuration config) {
        this.config = config;
    }

    /**
     * Sets up the event gateway.
     */
    public void setOperatorEventGateway(OperatorEventGateway operatorEventGateway) {
        this.eventGateway = operatorEventGateway;
    }

    /**
     * Sets up the mail executor.
     */
    public void setMailboxExecutor(MailboxExecutor executor) {
        this.executor = executor;
    }

    /**
     * Sets up the stream task.
     */
    public void setContainer(StreamTask<?, ?> container) {
        this.container = container;
    }

    /**
     * Sets up the stream config.
     */
    public void setStreamConfig(StreamConfig streamConfig) {
        this.streamConfig = streamConfig;
    }

    /**
     * Sets up the processing time service.
     */
    public void setProcessingTimeService(ProcessingTimeService processingTimeService) {
        this.processingTimeService = processingTimeService;
    }

    /**
     * Invoked when bounded source ends up.
     */
    public void endInput() throws Exception {
        this.inputEnded = true;
    }

    /**
     * Handles the operator event sent by the coordinator.
     *
     * @param event The event
     */
    public abstract void handleOperatorEvent(OperatorEvent event);
}