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

import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

@RunWith(Parameterized.class)
public class EmulatorSourceITCase extends EmulatorFixture {
    @Parameterized.Parameters(name = "{0}/{5}")
    public static Collection<Object[]> cases() {
        List<Object[]> cases = new ArrayList<>();
        for (Object[] type : types())
            if (Arrays.asList("bigint", "struct", "binary").contains(type[0]))
                for (String split : Arrays.asList("ROW_OFFSET", "BYTE_SIZE")) {
                    Object[] row = Arrays.copyOf(type, 6);
                    row[5] = split;
                    cases.add(row);
                }
        return cases;
    }

    private final String split;

    public EmulatorSourceITCase(String n, String o, String f, String l, String e, String split) {
        super(n, o, f, l, e);
        this.split = split;
    }

    @Override
    protected String extraOptions() {
        return ", 'splitstrategy'='" + split + "'";
    }

    @Test(timeout = 90000)
    public void boundedSourceReadsProjectionAndNull() throws Exception {
        sql("INSERT INTO " + table + " VALUES (1," + literal + "),(2,NULL)");
        Set<String> values = new HashSet<>();
        try (CloseableIterator<Row> rows =
                environment().executeSql("SELECT v FROM mc_rows").collect()) {
            while (rows.hasNext()) values.add(normalize(rows.next().getField(0)));
        }
        assertEquals(new HashSet<>(Arrays.asList(expected, "null")), values);
    }
}
