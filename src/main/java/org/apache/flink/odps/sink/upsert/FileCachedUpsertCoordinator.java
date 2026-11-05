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

package org.apache.flink.odps.sink.upsert;

import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.SessionStatus;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.write.WriterCommitMessage;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.sink.insert.FileCachedInsertCoordinator;
import org.apache.flink.odps.sink.common.OdpsWriteOptions;
import org.apache.flink.odps.sink.table.TableUpsertSessionImpl;
import org.apache.flink.odps.sink.utils.AsyncExecuteThread;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.flink.odps.table.OdpsOptions.*;

public class FileCachedUpsertCoordinator extends FileCachedInsertCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(FileCachedUpsertCoordinator.class);

    protected transient Map<String, Long> upsertCommitTimes;
    protected boolean enableMajorCompact;
    protected int compactThreshold = 1;

    public FileCachedUpsertCoordinator(String operatorName,
                                       Configuration config,
                                       Context context,
                                       OdpsConf odpsConf,
                                       TableIdentifier identifier,
                                       String partition,
                                       DataSchema dataSchema,
                                       boolean isDynamicPartition,
                                       boolean isGroupPartition,
                                       OdpsWriteOptions writeOptions) {
        super(operatorName, config, context, odpsConf, identifier, partition,
                dataSchema, isDynamicPartition, isGroupPartition, writeOptions);
        this.executeThreadNum = conf.getInteger(UPSERT_COMMIT_THREAD_NUM);
        this.enableMajorCompact = conf.getBoolean(UPSERT_MAJOR_COMPACT_ENABLE);
        this.compactThreshold = conf.getInteger(UPSERT_MAJOR_COMPACT_MIN_COMMITS);
    }

    @Override
    protected void initEnv() {
        super.initEnv();
        this.upsertCommitTimes = new HashMap<>();
    }

    @Override
    protected String initDynamicWriteSession(String partitionSpec) throws IOException {
        TableUpsertSessionImpl session = null;
        try {
            session = OdpsUtils.getOrCreateUpsertSession(tableIdentifier, partitionSpec, null, settings);

            return session.getId();
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }

    @Override
    protected void commitWriteSession(List<WriterCommitMessage> commitMessages, String partition, String sessionId, long totalRowCount) throws IOException {
        TableUpsertSessionImpl session = null;
        try {
            session = OdpsUtils.getUpsertSession(tableIdentifier, partition, sessionId, settings);
            if (session.getStatus().equals(SessionStatus.COMMITTED)) {
                LOG.info("Session has committed {}, partition {}", sessionId, partition);
                return;
            }

            LOG.info("Start to commit session {}, partition {}, row count {}", sessionId, partition, totalRowCount);

            long startTime = System.nanoTime();

            session.commit();

            LOG.info("Commit session success, session id {}, time taken ms: {}", sessionId,
                    NANOSECONDS.toMillis(System.nanoTime() - startTime));

            long commitTimes = upsertCommitTimes.getOrDefault(partition, 0L);
            upsertCommitTimes.put(partition, ++commitTimes);

            if (enableMajorCompact && commitTimes > compactThreshold) {
                upsertCommitTimes.remove(partition);
                executor.execute(() -> handleCompactEvent(partition),
                        "handle partition check event");
            }

        } catch (Exception e) {
            throw new IOException("Commit session " + sessionId + " failed!", e);
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }

    private void handleCompactEvent(String partition) {
        final AsyncExecuteThread.ExecuteRequest compactRequest = new AsyncExecuteThread.CompactRequest() {

            @Override
            public void execute() throws IOException {
                try {
                    long startTime = System.nanoTime();

                    OdpsUtils.majorCompact(odps, tableIdentifier, partition);

                    LOG.info("Checkpoint compact table partition {}, time taken ms: {}", partition,
                            NANOSECONDS.toMillis(System.nanoTime() - startTime));

                } catch (Exception e) {
                    throw new IOException(e);
                }
            }

            @Override
            public void requestDone(IOException ioex) {
                if (ioex != null) {
                    LOG.error("Checkpoint compact table partition {} failed",
                            partition, ioex);
                }
            }

            @Override
            public ExecuteType getExecuteType() {
                return ExecuteType.COMPACT;
            }

            @Override
            public long getCheckpointId() {
                return -1L;
            }

            @Override
            public String getPartition() {
                return partition;
            }

            @Override
            public String getSessionId() {
                return "";
            }
        };

        addExecuteRequest(partition, compactRequest);
    }
}
