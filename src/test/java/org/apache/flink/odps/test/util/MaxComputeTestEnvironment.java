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

import java.util.Map;

/** Explicit, environment-only identity for disposable real-service tests. */
public final class MaxComputeTestEnvironment {
    private MaxComputeTestEnvironment() {
    }

    public static OdpsConf getOdpsConf() {
        return fromEnvironment(System.getenv(),
                Boolean.parseBoolean(System.getProperty("maxcompute.e2e.allow-write", "false")));
    }

    static OdpsConf fromEnvironment(Map<String, String> env, boolean allowWrite) {
        if (!allowWrite) {
            throw new IllegalStateException("Real-service tests require -Dmaxcompute.e2e.allow-write=true");
        }
        String project = required(env, "MAXCOMPUTE_PROJECT");
        if (!project.matches("[A-Za-z][A-Za-z0-9_]*")) {
            throw new IllegalArgumentException("MAXCOMPUTE_PROJECT must be a project identifier");
        }
        String endpoint = env.get("MAXCOMPUTE_ENDPOINT");
        if (endpoint == null || endpoint.isEmpty()) {
            endpoint = required(env, "ODPS_ENDPOINT");
        }
        OdpsConf conf = new OdpsConf(required(env, "ODPS_ACCESS_ID"),
                required(env, "ODPS_ACCESS_KEY"), endpoint, project);
        conf.setSignatureCorporation("aliyun");
        String token = env.get("ODPS_SECURITY_TOKEN");
        if (token != null && !token.isEmpty()) {
            conf.setProperty("odps.access.security.token", token);
        }
        return conf;
    }

    private static String required(Map<String, String> env, String name) {
        String value = env.get(name);
        if (value == null || value.isEmpty()) {
            throw new IllegalStateException("Missing environment variable " + name);
        }
        return value;
    }
}
