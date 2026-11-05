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

package org.apache.flink.odps.test.util;

import static org.junit.Assert.*;

import com.aliyun.credentials.api.*;
import com.aliyun.odps.account.*;
import com.aliyun.odps.credentials.Credentials;

import org.apache.flink.odps.FlinkOdpsException;
import org.apache.flink.odps.util.*;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/** Offline contracts for local config, credential precedence and retry interruption. */
public class OdpsRuntimeContractsTest {
    @Test
    public void legacySdkReceivesConfiguredReadTimeout() {
        OdpsConf conf =
                new OdpsConf(
                        "dummy",
                        "dummy",
                        "http://127.0.0.1:1",
                        "test_project",
                        "http://127.0.0.1:1");
        org.apache.flink.configuration.Configuration options =
                new org.apache.flink.configuration.Configuration();
        options.set(
                org.apache.flink.odps.table.OdpsOptions.READ_TIMEOUT,
                java.time.Duration.ofSeconds(3));
        com.aliyun.odps.table.enviroment.EnvironmentSettings settings =
                org.apache.flink.odps.sink.table.TableUtils.getEnvironmentSettings(
                        OdpsUtils.getOdps(conf), conf.getTunnelEndpoint(), options);
        assertEquals(Integer.valueOf(3), settings.getRestOptions().get().getReadTimeout().get());
    }

    @Test
    public void configFilePreservesUnicodeAndCredentialPrecedence() throws Exception {
        Path dir = Files.createTempDirectory("odps-config-");
        Path file = dir.resolve("odps.conf");
        try {
            Files.write(
                    file,
                    Arrays.asList(
                            "odps.access.id=test-ak",
                            "odps.access.key=test-sk",
                            "odps.end.point=http://127.0.0.1:1",
                            "odps.project.name=test_project",
                            "odps.tunnel.end.point=http://127.0.0.1:1",
                            "fixture.label=中文"));
            OdpsConf conf = OdpsUtils.getOdpsConf(dir.toString());
            assertEquals("中文", conf.getProperty("fixture.label"));
            assertNull(conf.getProperty("odps.access.key"));
            assertSame(conf, OdpsUtils.getOdpsConf(dir.toString()));
            conf.setCredentialsProviderClass(String.class.getName());
            assertTrue(OdpsUtils.getDefaultAccount(conf) instanceof AliyunAccount);
            conf.setProperty("odps.access.security.token", "dummy-token");
            assertTrue(OdpsUtils.getDefaultAccount(conf) instanceof StsAccount);
            assertEquals(
                    "test-ak",
                    OdpsUtils.getCredentialsProvider(conf).getCredentials().getAccessKeyId());
        } finally {
            Files.deleteIfExists(file);
            Files.deleteIfExists(dir);
        }
    }

    @Test
    public void supportsProvidersWithAndWithoutParametersAndRejectsInvalidProviders() {
        OdpsConf conf = new OdpsConf(null, null, "http://127.0.0.1:1", "test_project");
        assertThrows(FlinkOdpsException.class, () -> OdpsUtils.getDefaultAccount(conf));
        conf.setCredentialsProviderClass(String.class.getName());
        assertThrows(FlinkOdpsException.class, () -> OdpsUtils.getDefaultAccount(conf));
        conf.setCredentialsProviderClass("missing.Provider");
        assertThrows(FlinkOdpsException.class, () -> OdpsUtils.getDefaultAccount(conf));
        conf.setCredentialsProviderClass(FixtureProvider.class.getName());
        assertTrue(OdpsUtils.getDefaultAccount(conf) instanceof AklessAccount);
        assertEquals(
                "dummy", OdpsUtils.getCredentialsProvider(conf).getCredentials().getAccessKeyId());
        conf.setCredentialsProviderParams(Collections.singletonMap("fixture", "value"));
        assertTrue(OdpsUtils.getDefaultAccount(conf) instanceof AklessAccount);
        assertEquals("value", FixtureProvider.parameters.get("fixture"));
    }

    @Test
    public void retriesTransientFailuresButPreservesExhaustionAndInterruption() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        assertEquals(
                "done",
                OdpsUtils.executeWithRetry(
                        () -> {
                            if (attempts.incrementAndGet() < 3) throw new IOException("transient");
                            return "done";
                        },
                        2,
                        0));
        assertEquals(3, attempts.get());
        IOException original = new IOException("permanent");
        assertSame(
                original,
                assertThrows(
                                IOException.class,
                                () ->
                                        OdpsUtils.executeWithRetry(
                                                () -> {
                                                    throw original;
                                                },
                                                0,
                                                0))
                        .getCause());
        Thread.currentThread().interrupt();
        try {
            IOException failure =
                    assertThrows(
                            IOException.class,
                            () ->
                                    OdpsUtils.executeWithRetry(
                                            () -> {
                                                throw original;
                                            },
                                            2,
                                            1));
            assertTrue(failure.getCause() instanceof InterruptedException);
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    public static final class FixtureProvider implements ICredentialsProvider {
        static Map<String, String> parameters;

        public FixtureProvider() {}

        public FixtureProvider(Map<String, String> params) {
            parameters = params;
        }

        public ICredentials getCredentials() {
            return new Credentials("dummy", "dummy", null);
        }

        public String getProviderName() {
            return "fixture";
        }

        public void close() {}
    }
}
