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
package org.apache.flink.odps.test.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.FlinkOdpsException;
import org.apache.flink.odps.table.OdpsDynamicTableSink;
import org.apache.flink.odps.table.OdpsDynamicTableSource;
import org.apache.flink.odps.table.factories.OdpsDynamicTableFactory;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class OdpsDynamicTableFactoryTest {
    private final OdpsDynamicTableFactory factory = new OdpsDynamicTableFactory(
            new OdpsConf("offline-id", "offline-key", "https://service.invalid/api", "offline_project"));

    @Test
    public void buildsSourceAndInsertSinkWithoutServiceAccess() {
        DynamicTableFactory.Context context = context(options(), false);
        assertTrue(factory.createDynamicTableSource(context) instanceof OdpsDynamicTableSource);
        assertTrue(factory.createDynamicTableSink(context) instanceof OdpsDynamicTableSink);
    }

    @Test
    public void acceptsMixedCaseConnectorOptions() {
        Map<String, String> options = options();
        options.put("TABLE.NAME", options.remove("table.name"));
        assertTrue(factory.createDynamicTableSource(context(options, false)) instanceof OdpsDynamicTableSource);
    }

    @Test
    public void upsertRequiresPrimaryKey() {
        Map<String, String> options = options(); options.put("sink.operation", "upsert");
        assertThrows(FlinkOdpsException.class,
                () -> factory.createDynamicTableSink(context(options, false)));
        assertTrue(factory.createDynamicTableSink(context(options, true)) instanceof OdpsDynamicTableSink);
    }

    @Test
    public void rejectsUnsupportedSinkOperation() {
        Map<String, String> options = options(); options.put("sink.operation", "unsupported");
        assertThrows(FlinkOdpsException.class,
                () -> factory.createDynamicTableSink(context(options, false)));
    }

    @Test
    public void rejectsUnknownOptionAndMissingTableName() {
        Map<String, String> unknown = options(); unknown.put("unknown.option", "value");
        assertThrows(ValidationException.class,
                () -> factory.createDynamicTableSource(context(unknown, false)));
        Map<String, String> missing = options(); missing.remove("table.name");
        assertThrows(ValidationException.class,
                () -> factory.createDynamicTableSource(context(missing, false)));
    }

    @Test
    public void rejectsPartialProjectEndpointOverride() {
        Map<String, String> options = options(); options.put("odps.project.name", "offline_project");
        assertThrows(IllegalArgumentException.class,
                () -> factory.createDynamicTableSource(context(options, false)));
    }

    private Map<String, String> options() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "maxcompute"); options.put("table.name", "offline_table");
        options.put("sink.operation", "insert");
        return options;
    }

    private DynamicTableFactory.Context context(Map<String, String> options, boolean primaryKey) {
        ResolvedSchema schema = new ResolvedSchema(Arrays.asList(
                Column.physical("id", DataTypes.BIGINT().notNull()),
                Column.physical("value", DataTypes.STRING())), Collections.emptyList(),
                primaryKey ? UniqueConstraint.primaryKey("pk", Collections.singletonList("id")) : null);
        CatalogTable table = CatalogTable.of(Schema.newBuilder().fromResolvedSchema(schema).build(),
                "offline", Collections.emptyList(), options);
        return new FactoryUtil.DefaultDynamicTableContext(
                ObjectIdentifier.of("offline", "offline_project", "offline_table"),
                new ResolvedCatalogTable(table, schema), Collections.emptyMap(), new Configuration(),
                getClass().getClassLoader(), false);
    }
}
