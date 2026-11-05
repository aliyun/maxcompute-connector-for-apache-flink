/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations.
 */
package org.apache.flink.odps.test.shim;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.Test;
import static org.junit.Assert.*;

/** Boots the actual SQL planner under the selected runtime and coverage agent, without a service. */
public class FlinkPlannerBootstrapTest {
    @Test
    public void sqlPlannerInitializesAndOptimizesAQuery() throws Exception {
        TableEnvironment environment = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        assertFalse(environment.explainSql("SELECT 1").isEmpty());
    }
}
