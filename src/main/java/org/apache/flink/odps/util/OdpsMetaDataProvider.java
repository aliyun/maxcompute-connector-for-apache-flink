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

package org.apache.flink.odps.util;

import com.aliyun.odps.*;
import com.aliyun.odps.table.TableIdentifier;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.flink.odps.FlinkOdpsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.odps.util.Constants.DEFAULT_ODPS_META_CACHE_EXPIRE_TIME;
import static org.apache.flink.odps.util.Constants.DEFAULT_ODPS_META_CACHE_SIZE;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utils to get odps meta data.
 */
public class OdpsMetaDataProvider {

    private static final Logger LOG = LoggerFactory.getLogger(OdpsMetaDataProvider.class);
    public LoadingCache<String, Optional<Project>> projectCache;
    public LoadingCache<PartitionPath, Optional<Partition>> partitionCache;
    public LoadingCache<TableIdentifier, Optional<Table>> tableCache;

    private final int cacheSize;
    private final int cacheExpireTime;
    private final Odps odps;

    public OdpsMetaDataProvider(Odps odps) {
        this(odps, DEFAULT_ODPS_META_CACHE_SIZE, DEFAULT_ODPS_META_CACHE_EXPIRE_TIME);
    }

    public OdpsMetaDataProvider(Odps odps, int cacheSize, int cacheExpireTime) {
        this.odps = odps;
        this.cacheSize = cacheSize;
        this.cacheExpireTime = cacheExpireTime;
        this.initMetaCache();
    }

    private CacheBuilder createCacheBuilder() {
        return CacheBuilder.newBuilder()
                .maximumSize(cacheSize)
                .expireAfterWrite(cacheExpireTime, TimeUnit.SECONDS);
    }

    private void initMetaCache() {
        CacheLoader<String, Optional<Project>> projectCacheLoader = new CacheLoader<String, Optional<Project>>() {
            @Override
            public Optional<Project> load(String projectName) throws Exception {
                try {
                    Project project = odps.projects().get(projectName);
                    project.reload();
                    return Optional.of(project);
                } catch (OdpsException e) {
                    LOG.error("load odps project failed: " + e.getMessage());
                    return Optional.empty();
                }
            }
        };
        projectCache = createCacheBuilder().build(projectCacheLoader);
        CacheLoader<TableIdentifier, Optional<Table>> tableCacheLoader = new CacheLoader<TableIdentifier, Optional<Table>>() {
            @Override
            public Optional<Table> load(TableIdentifier qualifiedTableName) throws Exception {
                try {
                    Table table = odps.tables().get(
                            qualifiedTableName.getProject(),
                            qualifiedTableName.getSchema(),
                            qualifiedTableName.getTable());
                    table.reload();
                    return Optional.of(table);
                } catch (OdpsException e) {
                    LOG.error("load odps table failed: " + e.getMessage());
                    return Optional.empty();
                }
            }
        };
        tableCache = createCacheBuilder().build(tableCacheLoader);

        CacheLoader<PartitionPath, Optional<Partition>> partitionCacheLoader = new CacheLoader<PartitionPath, Optional<Partition>>() {
            @Override
            public Optional<Partition> load(PartitionPath qualifiedPartition) throws Exception {
                try {
                    Table table = tableCache.get(TableIdentifier.of(
                            qualifiedPartition.getProjectName(),
                            qualifiedPartition.getSchemaName(),
                            qualifiedPartition.getTableName()))
                            .orElseThrow(() -> new FlinkOdpsException(qualifiedPartition.getProjectName()
                                    + "." + qualifiedPartition.getSchemaName()
                                    + "." + qualifiedPartition.getTableName() + " does not exist"));
                    Partition partition = table.getPartition(new PartitionSpec(qualifiedPartition.getPartitionSpec()));
                    partition.reload();
                    return Optional.of(partition);
                } catch (OdpsException e) {
                    LOG.info("load odps partition failed: " + e.getMessage());
                    return Optional.empty();
                }
            }
        };
        partitionCache = createCacheBuilder().build(partitionCacheLoader);
    }

    public Partition getPartition(String projectName, String schemaName, String tableName, String partitionSpec, boolean refresh) {
        checkNotNull(projectName, "projectName cannot be null");
        checkNotNull(tableName, "tableName cannot be null");
        checkNotNull(partitionSpec, "partitionSpec cannot be null");
        try {
            PartitionPath partitionPath = new PartitionPath(projectName, schemaName, tableName, partitionSpec);
            return getPartitionOption(partitionPath, refresh).orElse(null);
        } catch (ExecutionException e) {
            throw new FlinkOdpsException(e);
        }
    }

    public List<Partition> getPartitions(String projectName, String schemaName, String tableName, boolean refresh) {
        Table table = refresh ? getTable(projectName, schemaName, tableName, true) : getTable(projectName, schemaName, tableName);
        List<Partition> result = table.getPartitions();
        result.forEach(partition -> {
            partitionCache.put(new PartitionPath(projectName, schemaName, tableName, partition.getPartitionSpec().toString()),
                    Optional.of(partition));
        });
        return result;
    }

    public void loadPartitions(String projectName, String schemaName, String tableName) {
        Table table = getTable(projectName, schemaName, tableName);
        List<Partition> result = table.getPartitions();
        Set<String> partitionSpecs = new HashSet<>();
        result.forEach(partition -> {
            String part = partition.getPartitionSpec().toString();
            partitionSpecs.add(part);
            partitionCache.put(new PartitionPath(projectName, schemaName, tableName, part),
                    Optional.of(partition));
        });

        List<PartitionPath> invalidPartitions = partitionCache.asMap().keySet().stream().filter(partitionPath ->
                partitionPath.getProjectName().equals(projectName)
                && partitionPath.getSchemaName().equals(schemaName)
                && partitionPath.getTableName().equals(tableName)
                && !partitionSpecs.contains(partitionPath.getPartitionSpec())).collect(Collectors.toList());
        partitionCache.invalidateAll(invalidPartitions);
    }

    public List<Partition> getPartitions(String projectName, String schemaName, String tableName) {
        return this.getPartitions(projectName, schemaName, tableName, false);
    }

    public Table getTable(String projectName, String tableName, boolean refresh) {
        return this.getTable(projectName, "default", tableName, refresh);
    }

    public Table getTable(String projectName, String schemaName, String tableName, boolean refresh) {
        checkNotNull(projectName, "projectName cannot be null");
        checkNotNull(tableName, "tableName cannot be null");
        try {
            TableIdentifier tablePath = TableIdentifier.of(projectName, schemaName, tableName);
            Table odpsTable = getOdpsTableOption(tablePath, refresh)
                    .orElseThrow(() -> new FlinkOdpsException(projectName
                            + "." + schemaName
                            + "." + tableName
                            + " does not exist"));
            return odpsTable;
        } catch (ExecutionException e) {
            throw new FlinkOdpsException(e);
        }
    }

    public Table getTable(String projectName, String tableName) {
        return this.getTable(projectName, tableName, false);
    }

    public Table getTable(String projectName, String schemaName, String tableName) {
        return this.getTable(projectName, schemaName, tableName, false);
    }

    public Project getProject(String projectName, boolean refresh) {
        checkNotNull(projectName, "projectName cannot be null");
        try {
            Project project = getProjectOption(projectName, refresh)
                    .orElseThrow(() -> new FlinkOdpsException(projectName + " does not exist"));
            return project;
        } catch (ExecutionException e) {
            throw new FlinkOdpsException(e);
        }
    }

    public Project getProject(String projectName) {
        return this.getProject(projectName, false);
    }

    public TableSchema getTableSchema(String projectName, String tableName, boolean refresh) {
        checkNotNull(projectName, "projectName cannot be null");
        checkNotNull(tableName, "tableName cannot be null");
        return this.getTable(projectName, tableName, refresh).getSchema();
    }

    public TableSchema getTableSchema(String projectName, String schemaName, String tableName, boolean refresh) {
        checkNotNull(projectName, "projectName cannot be null");
        checkNotNull(tableName, "tableName cannot be null");
        return this.getTable(projectName, schemaName, tableName, refresh).getSchema();
    }

    private Optional<Table> getOdpsTableOption(TableIdentifier objectPath, boolean refresh) throws ExecutionException {
        if (refresh) {
            tableCache.invalidate(objectPath);
        }
        return tableCache.get(objectPath);
    }

    private Optional<Project> getProjectOption(String projectName, boolean refresh) throws ExecutionException {
        if (refresh) {
            projectCache.invalidate(projectName);
        }
        return projectCache.get(projectName);
    }

    private Optional<Partition> getPartitionOption(PartitionPath partitionPath, boolean refresh) throws ExecutionException {
        if (refresh) {
            partitionCache.invalidate(partitionPath);
        }
        return partitionCache.get(partitionPath);
    }
}
