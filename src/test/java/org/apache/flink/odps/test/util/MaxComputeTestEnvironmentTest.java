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
package org.apache.flink.odps.test.util;

import org.apache.flink.odps.util.OdpsConf;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class MaxComputeTestEnvironmentTest {
    @Test
    public void refusesServiceAccessWithoutExplicitOptIn() {
        assertThrows(IllegalStateException.class,
                () -> MaxComputeTestEnvironment.fromEnvironment(new HashMap<>(), false));
    }

    @Test
    public void usesInjectedEndpointAndSecurityToken() {
        Map<String, String> env = fixture();
        env.put("ODPS_SECURITY_TOKEN", "offline-token");
        OdpsConf conf = MaxComputeTestEnvironment.fromEnvironment(env, true);
        assertEquals("https://service.invalid/api", conf.getEndpoint());
        assertEquals("offline_project", conf.getProject());
        assertEquals("offline-token", conf.getProperty("odps.access.security.token"));
    }

    @Test
    public void failsOnMissingIdentityWithoutIncludingValuesInError() {
        Map<String, String> env = fixture();
        env.remove("ODPS_ACCESS_KEY");
        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> MaxComputeTestEnvironment.fromEnvironment(env, true));
        assertEquals("Missing environment variable ODPS_ACCESS_KEY", error.getMessage());
        assertFalse(error.getMessage().contains("offline-id"));
    }

    @Test
    public void rejectsSqlFragmentsInProjectName() {
        Map<String, String> env = fixture();
        env.put("MAXCOMPUTE_PROJECT", "project;drop table t");
        assertThrows(IllegalArgumentException.class,
                () -> MaxComputeTestEnvironment.fromEnvironment(env, true));
    }

    private static Map<String, String> fixture() {
        Map<String, String> env = new HashMap<>();
        env.put("MAXCOMPUTE_PROJECT", "offline_project");
        env.put("ODPS_ENDPOINT", "https://service.invalid/api");
        env.put("ODPS_ACCESS_ID", "offline-id");
        env.put("ODPS_ACCESS_KEY", "offline-key");
        return env;
    }
}
