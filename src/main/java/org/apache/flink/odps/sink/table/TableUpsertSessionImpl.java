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

import java.io.IOException;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.commons.util.RetryExceedLimitException;
import com.aliyun.odps.commons.util.RetryStrategy;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.table.DataFormat;
import com.aliyun.odps.table.SessionStatus;
import com.aliyun.odps.table.SessionType;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.ArrowOptions;
import com.aliyun.odps.table.configuration.DynamicPartitionOptions;
import com.aliyun.odps.table.configuration.RestOptions;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.utils.Preconditions;
import com.aliyun.odps.table.write.TableWriteCapabilities;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.impl.UpsertSessionImpl;
import org.apache.flink.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.aliyun.odps.tunnel.TunnelConstants.SESSION_STATUS_COMMITTED;
import static com.aliyun.odps.tunnel.TunnelConstants.SESSION_STATUS_COMMITTING;
import static com.aliyun.odps.tunnel.TunnelConstants.SESSION_STATUS_CRITICAL;
import static com.aliyun.odps.tunnel.TunnelConstants.SESSION_STATUS_EXPIRED;
import static com.aliyun.odps.tunnel.TunnelConstants.SESSION_STATUS_NORMAL;

public class TableUpsertSessionImpl extends TableUpsertSessionBase {

    private static final Logger LOG = LoggerFactory.getLogger(TableUpsertSessionImpl.class);

    protected transient TableTunnel.UpsertSession session;
    protected transient RetryStrategy commitRetry;
    protected boolean isCommitted;
    protected boolean isClosed;
    protected int maxRetries = 3;

    public TableUpsertSessionImpl(TableIdentifier identifier,
                                  PartitionSpec partitionSpec,
                                  boolean overwrite,
                                  DynamicPartitionOptions dynamicPartitionOptions,
                                  ArrowOptions arrowOptions,
                                  TableWriteCapabilities capabilities,
                                  EnvironmentSettings settings) throws IOException {
        super(identifier, partitionSpec, overwrite,
                dynamicPartitionOptions, arrowOptions, capabilities, settings);
        this.isCommitted = false;
        this.isClosed = false;
        this.commitRetry =
                new RetryStrategy(maxRetries, new Random().nextInt(10) + 10, RetryStrategy.BackoffStrategy.LINEAR_BACKOFF);
    }

    public TableUpsertSessionImpl(TableIdentifier identifier,
                                  PartitionSpec partitionSpec,
                                  String sessionId,
                                  EnvironmentSettings settings) throws IOException {
        super(identifier, partitionSpec, sessionId, settings);
        this.isCommitted = false;
        this.isClosed = false;
        this.commitRetry =
                new RetryStrategy(maxRetries, new Random().nextInt(10) + 10, RetryStrategy.BackoffStrategy.LINEAR_BACKOFF);
    }

    @Override
    protected void initSession() throws IOException {
        int attempt = 0;
        while (true) {
            try {
                TableTunnel tunnel = TableUtils.getTableTunnel(settings);
                RestOptions restOptions = settings.getRestOptions().orElse(RestOptions.newBuilder().build());
                this.session = ((UpsertSessionImpl.Builder) tunnel.buildUpsertSession(identifier.getProject(), identifier.getTable()))
                        .setConfig(tunnel.getConfig())
                        .setSchemaName(identifier.getSchema())
                        .setPartitionSpec(targetPartitionSpec)
                        .setSlotNum(settings.getSlotNum().orElse(8L))
                        .setCommitTimeout(restOptions.getAsyncTimeoutInSeconds().orElse(600) * 1000L)
                        .setReadTimeout(restOptions.getReadTimeout().orElse(300) * 1000L)
                        .setConnectTimeout(restOptions.getConnectTimeout().orElse(180) * 1000L)
                        .setNetworkThreadNum(restOptions.getUpsertNetworkNum().orElse(1))
                        .setConcurrentNum(restOptions.getUpsertConcurrentNum().orElse(2))
                        .build();
                this.sessionId = session.getId();
                this.sessionStatus = convertSessionStatus(session.getStatus());
                break;
            } catch (Exception e) {
                attempt++;
                if (attempt > maxRetries) {
                    throw new IOException("Create session failed after retries", e);
                }
                try {
                    if (e.getMessage().contains("Quota Exceeded")
                            || e.getMessage().contains("SlotExceeded")) {
                        LOG.warn("Create session retry after exception: " + e.getMessage());
                        Thread.sleep(ThreadLocalRandom.current().nextLong(5000));
                    } else {
                        Thread.sleep(1000);
                    }
                } catch (InterruptedException ie) { ;
                    throw new IOException("Create session retry interrupted", ie);
                }
            }
        }
    }

    @Override
    protected void reloadSession() throws IOException {
        try {
            TableTunnel tunnel = TableUtils.getTableTunnel(settings);
            RestOptions restOptions = settings.getRestOptions().orElse(RestOptions.newBuilder().build());
            this.session = ((UpsertSessionImpl.Builder) tunnel.buildUpsertSession(identifier.getProject(), identifier.getTable()))
                            .setConfig(tunnel.getConfig())
                            .setSchemaName(identifier.getSchema())
                            .setPartitionSpec(targetPartitionSpec)
                            .setUpsertId(getId())
                            .setCommitTimeout(restOptions.getAsyncTimeoutInSeconds().orElse(600) * 1000L)
                            .setReadTimeout(restOptions.getReadTimeout().orElse(300) * 1000L)
                            .setConnectTimeout(restOptions.getConnectTimeout().orElse(180) * 1000L)
                            .setNetworkThreadNum(restOptions.getUpsertNetworkNum().orElse(1))
                            .setConcurrentNum(restOptions.getUpsertConcurrentNum().orElse(2))
                            .build();
            this.sessionStatus = convertSessionStatus(session.getStatus());
        } catch (TunnelException e) {
            throw new IOException(e);
        }
    }

    public UpsertWriter<ArrayRecord> createUpsertWriter(TableUpsertWriterOptions options) throws IOException {
        Preconditions.checkNotNull(session, "Upsert session", "required");
        return new TableUpsertWriterImpl(session, options);
    }

    public void commit() throws IOException {
        Preconditions.checkNotNull(session, "Upsert session", "required");
        if (isCommitted) {
            return;
        }
        commitRetry.reset();
        while (true) {
            try {
                session.commit(false);
                sessionStatus = convertSessionStatus(session.getStatus());
                break;
            } catch (Exception exception) {
                LOG.error(String.format("Commit error, session id = %s, retry times = %d",
                        session.getId(), commitRetry.getAttempts()), exception);
                try {
                    final Optional<InterruptedException> interruptedException =
                            ExceptionUtils.findThrowable(
                                    exception, InterruptedException.class);
                    if (interruptedException.isPresent()) {
                        throw exception;
                    }

                    sessionStatus = convertSessionStatus(session.getStatus());
                    if (sessionStatus.equals(SessionStatus.CRITICAL)
                            || sessionStatus.equals(SessionStatus.EXPIRED)
                            || sessionStatus.equals(SessionStatus.UNKNOWN)) {
                        throw new TunnelException(String.format("Commit error, session id = %s, session status = %s",
                                session.getId(), commitRetry.getAttempts()), sessionStatus.toString());
                    } else {
                        commitRetry.onFailure(exception);
                    }
                } catch (RetryExceedLimitException | InterruptedException | TunnelException e) {
                    throw new IOException(e);
                }
            }
        }
        isCommitted = true;
    }

    public void close() {
        if (isClosed) {
            return;
        }
        session.close();
        isClosed = true;
    }

    @Override
    public boolean supportsDataFormat(DataFormat dataFormat) {
        return dataFormat.getType().equals(DataFormat.Type.RECORD);
    }

    @Override
    public SessionType getType() {
        return SessionType.UPSERT;
    }

    private SessionStatus convertSessionStatus(String status) {
        if (status.equalsIgnoreCase(SESSION_STATUS_NORMAL)) {
            return SessionStatus.NORMAL;
        } else if (status.equalsIgnoreCase(SESSION_STATUS_COMMITTING)) {
            return SessionStatus.COMMITTING;
        } else if (status.equalsIgnoreCase(SESSION_STATUS_COMMITTED)) {
            return SessionStatus.COMMITTED;
        } else if (status.equalsIgnoreCase(SESSION_STATUS_EXPIRED)) {
            return SessionStatus.EXPIRED;
        } else if (status.equalsIgnoreCase(SESSION_STATUS_CRITICAL)) {
            return SessionStatus.CRITICAL;
        } else {
            return SessionStatus.UNKNOWN;
        }
    }

}
