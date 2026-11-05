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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

/** Two SQL sinks contend for the same source and service under bounded backpressure. */
@RunWith(Parameterized.class)
public class EmulatorConcurrentSqlITCase extends EmulatorWriteHarness {
    private final String writer;
    private final int parallelism;

    @Parameterized.Parameters(name = "{0}/p={1}")
    public static Collection<Object[]> cases() {
        List<Object[]> cases = new ArrayList<>();
        for (String writer :
                Arrays.asList(
                        "record",
                        "cached",
                        "cached-dynamic",
                        "grouped-cached-dynamic",
                        "modern",
                        "upsert-record",
                        "upsert-cached",
                        "upsert-modern"))
            for (int p : new int[] {4, 8, 16})
                if ((writer + "/" + p).matches(System.getProperty("emulator.stress.filter", ".*")))
                    cases.add(new Object[] {writer, p});
        if (cases.isEmpty()) throw new IllegalArgumentException("No stress scenario matched");
        return cases;
    }

    public EmulatorConcurrentSqlITCase(String writer, int parallelism) {
        this.writer = writer;
        this.parallelism = parallelism;
    }

    @Test(timeout = 600000)
    public void parallelSqlWritersPreserveEveryRowAndReleaseResources() throws Exception {
        writeScenario(
                "stress_" + writer + "_p" + parallelism,
                writer.replace("upsert-", ""),
                writer.startsWith("upsert-"),
                parallelism,
                Integer.getInteger("emulator.stress.rows", 4096),
                "write",
                "delay",
                2);
    }
}
