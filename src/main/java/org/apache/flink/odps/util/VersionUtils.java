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

package org.apache.flink.odps.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author dingxin ()
 */
public class VersionUtils {
    private static final Logger LOG = LoggerFactory.getLogger(VersionUtils.class);

    private static Properties properties;

    static {
        properties = new Properties();
        try (InputStream inputStream = VersionUtils.class.getClassLoader().getResourceAsStream("version.properties")) {
            if (inputStream != null) {
                properties.load(inputStream);
            } else {
                System.err.println("version.properties file not found");
            }
        } catch (IOException e) {
            System.err.println("Error loading version.properties file: " + e.getMessage());
        }
    }

    public static String getOdpsSdkVersion() {
        return properties.getProperty("odps.sdk.version");
    }

    public static String getFlinkVersion() {
        return properties.getProperty("flink.version");
    }

    public static String getConnectorVersion() {
        return properties.getProperty("connector.version");
    }

    public static void logVersions() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append("+----------------------------------------+\n");
        sb.append("|          Version Information           |\n");
        sb.append("+----------------------------------------+\n");
        sb.append(String.format("| ODPS SDK Version: %-21s |\n", getOdpsSdkVersion()));
        sb.append(String.format("| Flink Version: %-23s |\n", getFlinkVersion()));
        sb.append(String.format("| Connector Version: %-20s |\n", getConnectorVersion()));
        sb.append("+----------------------------------------+\n");

        // 使用 INFO 级别输出日志
        LOG.info(sb.toString());
    }
}
