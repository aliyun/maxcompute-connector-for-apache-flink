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

package org.apache.flink.odps.sink.writer;

import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.TableIdentifier;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.sink.common.WriteOperationType;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.io.Serializable;

public class OdpsWriterFactory implements Serializable {

    public static DataWriter<RowData> createOdpsTableWriter(
            Configuration config,
            OdpsConf odpsConf,
            TableIdentifier identifier,
            String partition,
            DataSchema schema,
            WriteOperationType type,
            PartitionWriterInfo writerInfo,
            int subtaskIndex,
            int taskParallel,
            RowType rowType) throws IOException {
        String sessionId = null;
        if (writerInfo != null) {
            sessionId = writerInfo.getSessionId();
        }

        if (type.equals(WriteOperationType.UPSERT)) {
            return new OdpsUpsertWriter(config, odpsConf,
                    identifier,
                    partition,
                    schema,
                    sessionId,
                    rowType);
        } else if (type.equals(WriteOperationType.INSERT)) {
            return new OdpsInsertWriter(config, odpsConf,
                    identifier,
                    partition,
                    schema,
                    sessionId,
                    subtaskIndex,
                    taskParallel,
                    rowType);
        } else {
            throw new UnsupportedOperationException("Unsupported write operation: " + type);
        }
    }
}