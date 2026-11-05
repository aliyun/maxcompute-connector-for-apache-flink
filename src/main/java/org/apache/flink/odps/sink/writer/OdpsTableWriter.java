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

import com.aliyun.odps.Odps;
import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.sink.common.OdpsRecordConverter;
import org.apache.flink.odps.sink.table.TableUtils;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.odps.util.RowDataToOdpsConverters;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

public abstract class OdpsTableWriter implements DataWriter<RowData> {

    protected TableIdentifier tableIdentifier;

    protected EnvironmentSettings settings;

    protected OdpsRecordConverter converter;

    protected RowType rowType;

    protected OdpsConf odpsConf;

    protected DataSchema dataSchema;

    protected Odps odps;

    protected String partition;

    protected Configuration conf;

    public OdpsTableWriter(Configuration config,
                           OdpsConf odpsConf,
                           TableIdentifier identifier,
                           String partition,
                           DataSchema schema,
                           RowType rowType) {
        this.conf = config;
        this.odpsConf = odpsConf;
        this.tableIdentifier = identifier;
        this.dataSchema = schema;
        this.odps = OdpsUtils.getOdps(odpsConf);
        this.settings = TableUtils.getEnvironmentSettings(odps, odpsConf.getTunnelEndpoint(), conf);
        this.converter = RowDataToOdpsConverters.createRecordConverter(rowType, dataSchema);
        this.partition = partition;
        this.rowType = rowType;
    }
}
