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

package org.apache.flink.odps.source.enumerator;

import com.aliyun.odps.Odps;
import com.aliyun.odps.table.TableIdentifier;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.odps.source.split.OdpsSourceSplit;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsUtils;

/**
 * The splits enumerator for Odps partitions, which may be a static enumerator with given partitions
 * or a continuous enumerator scheduled periodically.
 */
public abstract class OdpsSplitEnumeratorBase<CheckpointT>
        implements SplitEnumerator<OdpsSourceSplit, CheckpointT> {

    protected final OdpsConf odpsConf;
    protected final TableIdentifier odpsTable;
    protected Odps odps;

    public OdpsSplitEnumeratorBase(OdpsConf odpsConf, TableIdentifier odpsTable) {
        this.odpsConf = odpsConf;
        this.odpsTable = odpsTable;
    }

    protected Odps getOdps() {
        if (odps == null) {
            this.odps = OdpsUtils.getOdps(this.odpsConf);
        }
        return odps;
    }
}
