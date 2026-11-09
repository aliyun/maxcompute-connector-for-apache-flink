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

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/** The supported spill-to-disk path must progress under the legacy mailbox reproducer. */
@RunWith(Parameterized.class)
public class EmulatorFileCachedMailboxITCase extends EmulatorInsertMailboxReproducer {
    private final boolean dynamic;
    private final int parallelism;

    @Parameterized.Parameters(name = "dynamic={0}/p={1}")
    public static Collection<Object[]> cases() {
        return Arrays.asList(new Object[][] {{false, 1}, {true, 3}});
    }

    public EmulatorFileCachedMailboxITCase(boolean dynamic, int parallelism) {
        this.dynamic = dynamic;
        this.parallelism = parallelism;
    }

    @Override
    protected boolean dynamic() {
        return dynamic;
    }

    @Override
    protected int parallelism() {
        return parallelism;
    }

    @Override
    protected boolean fileCached() {
        return true;
    }
}
