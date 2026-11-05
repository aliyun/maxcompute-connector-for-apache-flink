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
 * Modified for the MaxCompute Flink connector; see NOTICE for upstream attribution.
 */

package org.apache.flink.odps.table;

import org.apache.flink.util.StringUtils;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;

public class OdpsTablePath {

    private final String projectName;
    private final String tableName;
    private final String schemaName;

    public OdpsTablePath(String projectName, String tableName) {
        this(projectName, null, tableName);
    }

    public OdpsTablePath(String projectName, String schemaName, String tableName) {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(projectName));
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(tableName));

        this.projectName = projectName;
        this.tableName = tableName;
        this.schemaName = schemaName;
    }

    public static OdpsTablePath fromTablePath(String tablePath) {
        if (tablePath.contains(".")) {
            String[] path = tablePath.split("\\.");

            checkArgument(
                    path.length == 2 || path.length == 3,
                    String.format(
                            "Table name '%s' is not valid. The parsed length is %d",
                            tablePath, path.length));
            if (path.length == 2) {
                return new OdpsTablePath(path[0], path[1]);
            } else {
                return new OdpsTablePath(path[0], path[1], path[2]);
            }
        } else {
            throw new IllegalArgumentException(
                    String.format("Table name '%s' is not valid. Please set default project", tablePath));
        }
    }

    public static OdpsTablePath fromTablePath(String defaultProject, String tablePath, boolean schemaEnable) {
        if (tablePath.contains(".")) {
            String[] path = tablePath.split("\\.");

            checkArgument(
                    path.length == 2 || path.length == 3,
                    String.format(
                            "Table name '%s' is not valid. The parsed length is %d",
                            tablePath, path.length));

            if (schemaEnable) {
                if (path.length == 2) {
                    return new OdpsTablePath(defaultProject, path[0], path[1]);
                } else {
                    return new OdpsTablePath(path[0], path[1], path[2]);
                }
            } else {
                if (path.length == 3) {
                    throw new IllegalArgumentException(
                            String.format("Table name '%s' is not valid. Please set schema enable true", tablePath));
                } else {
                    return new OdpsTablePath(path[0], path[1]);
                }
            }
        } else {
            if (schemaEnable) {
                throw new IllegalArgumentException(
                        String.format("Table name '%s' is not valid. Please set schema", tablePath));
            } else {
                return new OdpsTablePath(defaultProject, tablePath);
            }
        }
    }

    public static String toTablePath(String projectName, String tableName) {
        return new OdpsTablePath(projectName, tableName).getFullPath();
    }

    public static String toTablePath(String projectName, String schemaName, String tableName) {
        return new OdpsTablePath(projectName, schemaName, tableName).getFullPath();
    }

    public String getFullPath() {
        if (schemaName == null) {
            return String.format("%s.%s", projectName, tableName);
        } else {
            return String.format("%s.%s.%s", projectName, schemaName, tableName);
        }
    }

    public String getProjectName() {
        return projectName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    @Override
    public String toString() {
        return getFullPath();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        OdpsTablePath that = (OdpsTablePath) o;
        return Objects.equals(projectName, that.projectName)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(schemaName, that.schemaName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(projectName, schemaName, tableName);
    }
}
