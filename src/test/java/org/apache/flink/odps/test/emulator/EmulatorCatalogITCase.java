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

import org.apache.flink.table.catalog.ObjectPath;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

@RunWith(Parameterized.class)
public class EmulatorCatalogITCase extends EmulatorFixture {
    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> cases() {
        List<Object[]> cases = new ArrayList<>();
        for (Object[] type : types()) {
            if (Arrays.asList("bigint", "array", "struct").contains(type[0])) cases.add(type);
        }
        return cases;
    }

    public EmulatorCatalogITCase(String n, String o, String f, String l, String e) {
        super(n, o, f, l, e);
    }

    @Test(timeout = 90000)
    public void catalogDiscoversSchemaAndTable() throws Exception {
        ObjectPath path = new ObjectPath("test_project", table);
        assertTrue(catalog.tableExists(path));
        assertTrue(catalog.listTables("test_project").contains(table));
        assertEquals(2, catalog.getTable(path).getUnresolvedSchema().getColumns().size());
    }

    @Test(timeout = 90000)
    public void createDropAndRefreshMetadataCache() throws Exception {
        ObjectPath path = new ObjectPath("test_project", table);
        org.apache.flink.table.catalog.CatalogBaseTable definition = catalog.getTable(path);
        catalog.dropTable(path, false);
        assertFalse(catalog.tableExists(path));
        assertThrows(
                org.apache.flink.table.catalog.exceptions.TableNotExistException.class,
                () -> catalog.getTable(path));
        catalog.createTable(path, definition, false);
        assertTrue(catalog.tableExists(path));
        assertEquals(2, catalog.getTable(path).getSchema().getFieldCount());
        catalog.createTable(path, definition, true);
        assertTrue(catalog.listDatabases().contains("test_project"));
        assertNotNull(catalog.getDatabase("test_project"));
        assertNotNull(catalog.getFactory().get());
    }

    @Test(timeout = 90000)
    public void partitionLifecycleAndPrefixListing() throws Exception {
        sql("DROP TABLE " + table);
        sql("CREATE TABLE " + table + " (id BIGINT,v " + odpsType + ") PARTITIONED BY (ds STRING)");
        ObjectPath path = new ObjectPath("test_project", table);
        assertTrue(catalog.tableExists(path));
        org.apache.flink.table.catalog.CatalogPartitionSpec spec =
                new org.apache.flink.table.catalog.CatalogPartitionSpec(
                        Collections.singletonMap("ds", "20261012"));
        catalog.createPartition(
                path,
                spec,
                new org.apache.flink.table.catalog.CatalogPartitionImpl(
                        Collections.emptyMap(), "fixture"),
                false);
        assertTrue(catalog.partitionExists(path, spec));
        assertEquals(Collections.singletonList(spec), catalog.listPartitions(path));
        assertEquals(Collections.singletonList(spec), catalog.listPartitions(path, spec));
        assertNotNull(catalog.getPartition(path, spec));
        catalog.dropPartition(path, spec, false);
        assertFalse(catalog.partitionExists(path, spec));
        assertTrue(catalog.listPartitions(path).isEmpty());
    }
}
