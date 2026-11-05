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
 */

package org.apache.flink.odps.test.emulator;

import static org.junit.Assert.*;

import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.*;
import org.apache.flink.table.catalog.stats.*;
import org.junit.Test;

import java.util.*;

public class EmulatorCatalogMutationITCase extends EmulatorFixture {
    public EmulatorCatalogMutationITCase() {
        super("catalogmutations", "BIGINT", "BIGINT", "42", "42");
    }

    @Test
    public void renameRefreshesBothPositiveAndNegativeMetadataCaches() throws Exception {
        ObjectPath before = new ObjectPath("test_project", table);
        String renamed = table + "_renamed";
        ObjectPath after = new ObjectPath("test_project", renamed);
        try {
            assertTrue(catalog.tableExists(before));
            assertFalse(catalog.tableExists(after));
            catalog.renameTable(before, renamed, false);
            assertFalse(catalog.tableExists(before));
            assertTrue(catalog.tableExists(after));
            assertEquals(2, catalog.getTable(after).getSchema().getFieldCount());
            assertThrows(
                    TableAlreadyExistException.class,
                    () -> catalog.renameTable(after, renamed, false));
            assertThrows(
                    TableNotExistException.class,
                    () -> catalog.renameTable(before, table + "_missing", false));
            catalog.renameTable(before, table + "_missing", true);
        } finally {
            sql("DROP TABLE IF EXISTS " + renamed);
        }
    }

    @Test
    public void invalidPartitionOperationsFailAndPrefixOrderIsPreserved() throws Exception {
        ObjectPath path = new ObjectPath("test_project", table);
        CatalogPartitionSpec empty = new CatalogPartitionSpec(Collections.emptyMap());
        assertThrows(TableNotPartitionedException.class, () -> catalog.listPartitions(path));
        sql("DROP TABLE " + table);
        sql(
                "CREATE TABLE "
                        + table
                        + "(id BIGINT,v BIGINT) PARTITIONED BY(ds STRING,region STRING)");
        catalog.open();
        CatalogPartition partition = new CatalogPartitionImpl(Collections.emptyMap(), "fixture");
        Map<String, String> values = new LinkedHashMap<>();
        values.put("region", "cn");
        values.put("ds", "a");
        CatalogPartitionSpec spec = new CatalogPartitionSpec(values);
        assertThrows(
                PartitionSpecInvalidException.class,
                () -> catalog.createPartition(path, empty, partition, false));
        catalog.createPartition(path, spec, partition, false);
        assertThrows(
                PartitionAlreadyExistsException.class,
                () -> catalog.createPartition(path, spec, partition, false));
        catalog.createPartition(path, spec, partition, true);
        assertEquals(
                Collections.singletonList(spec),
                catalog.listPartitions(
                        path, new CatalogPartitionSpec(Collections.singletonMap("ds", "a"))));
        assertTrue(
                catalog.listPartitions(
                                path, new CatalogPartitionSpec(Collections.singletonMap("ds", "b")))
                        .isEmpty());
        assertNotNull(catalog.getPartitionStatistics(path, spec));
        catalog.dropPartition(path, spec, false);
        assertThrows(PartitionNotExistException.class, () -> catalog.getPartition(path, spec));
        catalog.dropPartition(path, spec, true);
    }

    @Test
    public void unsupportedCatalogMutationsAreExplicitAndMissingTablesStayMissing()
            throws Exception {
        ObjectPath path = new ObjectPath("test_project", table);
        CatalogBaseTable definition = catalog.getTable(path);
        assertNotNull(catalog.getTableStatistics(path));
        assertTrue(catalog.listViews("test_project").isEmpty());
        assertThrows(CatalogException.class, () -> catalog.alterTable(path, definition, false));
        assertThrows(
                CatalogException.class,
                () ->
                        catalog.createDatabase(
                                "fixture",
                                new CatalogDatabaseImpl(Collections.emptyMap(), ""),
                                false));
        assertThrows(CatalogException.class, () -> catalog.dropDatabase("fixture", false));
        assertThrows(
                UnsupportedOperationException.class,
                () ->
                        catalog.createFunction(
                                path, new CatalogFunctionImpl("java.lang.String"), false));
        assertThrows(
                UnsupportedOperationException.class,
                () -> catalog.alterTableStatistics(path, CatalogTableStatistics.UNKNOWN, false));
        assertTrue(catalog.listFunctions("test_project").isEmpty());
        assertFalse(catalog.functionExists(path));
        assertThrows(FunctionNotExistException.class, () -> catalog.getFunction(path));
        ObjectPath missing = new ObjectPath("test_project", table + "_missing");
        assertThrows(TableNotExistException.class, () -> catalog.dropTable(missing, false));
        catalog.dropTable(missing, true);
        assertFalse(catalog.tableExists(missing));
    }
}
