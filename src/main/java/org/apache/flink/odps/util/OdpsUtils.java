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

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.aliyun.credentials.api.ICredentialsProvider;
import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Job;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Partition;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AklessAccount;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.account.AliyunRequestSigner;
import com.aliyun.odps.account.StsAccount;
import com.aliyun.odps.credentials.StaticCredentialProvider;
import com.aliyun.odps.data.Char;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.Varchar;
import com.aliyun.odps.storage.MaxStorageClient;
import com.aliyun.odps.storage.settings.HttpSettings;
import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.SessionStatus;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.CompressionCodec;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.write.TableBatchWriteSession;
import com.aliyun.odps.table.write.TableStreamingWriteSession;
import com.aliyun.odps.table.write.TableWriteSessionBuilder;
import com.aliyun.odps.task.MergeTask;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.tunnel.io.CompressOption.CompressAlgorithm;
import com.aliyun.odps.type.AbstractCharTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.google.gson.GsonBuilder;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.FlinkOdpsException;
import org.apache.flink.odps.schema.OdpsColumn;
import org.apache.flink.odps.sink.table.TableUpsertSessionImpl;
import org.apache.flink.odps.table.OdpsOptions;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.flink.odps.util.Constants.ODPS_ACCESS_ID;
import static org.apache.flink.odps.util.Constants.ODPS_ACCESS_KEY;
import static org.apache.flink.odps.util.Constants.ODPS_CONF_DIR;
import static org.apache.flink.odps.util.Constants.ODPS_END_POINT;
import static org.apache.flink.odps.util.Constants.ODPS_PROJECT_NAME;
import static org.apache.flink.odps.util.Constants.ODPS_TUNNEL_END_POINT;
import static org.apache.flink.odps.util.Constants.TUNNEL_TABLE_API_PROVIDER;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class OdpsUtils {
    private static final Logger LOG = LoggerFactory.getLogger(OdpsUtils.class);

    private static URL odpsConfUrl;
    private static OdpsConf odpsConf;

    public static OdpsConf getOdpsConf() {
        String confPath = System.getenv(ODPS_CONF_DIR);
        return getOdpsConf(confPath);
    }

    public synchronized static OdpsConf getOdpsConf(String confPath) {
        URL url = null;
        if (confPath == null) {
            confPath = System.getenv(ODPS_CONF_DIR);
        }
        if (confPath != null && !confPath.trim().isEmpty()) {
            File confFile = new File(confPath, "odps.conf");
            if (confFile.exists()) {
                try {
                    url = confFile.toURI().toURL();
                } catch (MalformedURLException e) {
                    LOG.error("cannot find user defined odps config file in " + confPath);
                }
            } else {
                LOG.error("cannot find user defined odps config file in " + confPath);
            }
        }
        if (url == null) {
            url = OdpsConf.class.getClassLoader().getResource("odps.conf");
        }

        if (odpsConfUrl != null && odpsConfUrl.equals(url)) {
            return odpsConf;
        }

        if (url != null) {
            Properties props = new Properties();
            try {
                InputStreamReader reader = new InputStreamReader(url.openStream(), StandardCharsets.UTF_8);
                props.load(reader);
            } catch (IOException ex) {
                LOG.error("load odps default configs failed.");
                return null;
            }

            OdpsConf odpsConf = new OdpsConf(props.getProperty(ODPS_ACCESS_ID), props.getProperty(ODPS_ACCESS_KEY),
                    props.getProperty(ODPS_END_POINT), props.getProperty(ODPS_PROJECT_NAME),
                    props.getProperty(ODPS_TUNNEL_END_POINT, ""));
            props.stringPropertyNames().stream().filter(OdpsUtils::validateProperty).forEach(
                    property -> odpsConf.setProperty(property, props.getProperty(property)));
            OdpsUtils.odpsConf = odpsConf;
            OdpsUtils.odpsConfUrl = url;
            return odpsConf;
        } else {
            LOG.error("cannot find user defined odps config file in classpath");
            return null;
        }
    }

    private static boolean validateProperty(String propertyName) {
        return propertyName != null && !propertyName.equals(ODPS_ACCESS_ID) && !propertyName.equals(ODPS_ACCESS_KEY)
                && !propertyName.equals(ODPS_END_POINT) && !propertyName.equals(ODPS_PROJECT_NAME)
                && !propertyName.equals(ODPS_TUNNEL_END_POINT);
    }

    public static Odps getOdps(OdpsConf odpsConf) {
        Odps odps = new Odps(getDefaultAccount(odpsConf));
        odps.setEndpoint(odpsConf.getEndpoint());
        odps.setDefaultProject(odpsConf.getProject());

        if (!"aliyun".equalsIgnoreCase(odpsConf.getSignatureCorporation())) {
            LOG.info("Modify signature corporation to {}", odpsConf.getSignatureCorporation());
            // FIXME：这会让 Odps 签名全局切换为新值，对于主权云场景，这是 OK 的。但后续应当修改为实例变量
            AliyunRequestSigner.setCorporation(odpsConf.getSignatureCorporation());
        }
        return odps;
    }

    public static MaxStorageClient getMaxStorageClient(OdpsConf odpsConf, Configuration configuration) {
        // RootAllocator rootAllocator = ArrowUtils.getRootAllocator();
        return MaxStorageClient.builder()
            .endpoint(odpsConf.getEndpoint())
            .tunnelEndpoint(odpsConf.getTunnelEndpoint())
            .project(odpsConf.getProject())
            .credentialsProvider(getCredentialsProvider(odpsConf))
            .httpSettings(HttpSettings.newBuilder()
                              .withReadTimeout(configuration.get(OdpsOptions.READ_TIMEOUT))
                              .build())
            // .bufferAllocator(rootAllocator)
            .build();
    }

    public static ICredentialsProvider getCredentialsProvider(OdpsConf odpsConf) {
        Account account = getDefaultAccount(odpsConf);
        if (account instanceof AklessAccount) {
            return ((AklessAccount)account).getCredentialsProvider();
        } else {
            return StaticCredentialProvider.of(account.getCredentials());
        }
    }

    /**
     * 获取默认的 ODPS 账户对象，并调整了身份验证方式的优先级。
     * <p>
     * 优先级顺序：
     * 1. AccessKey ID / SecretKey (+ 可选的 STS Token)
     * 2. Credentials Provider
     *
     * @param odpsConf ODPS 配置对象
     * @return 创建的 Account 对象
     * @throws FlinkOdpsException 如果没有提供任何有效的凭证信息
     */
    public static Account getDefaultAccount(OdpsConf odpsConf) {
        String accessId = odpsConf.getAccessId();
        String accessKey = odpsConf.getAccessKey();
        String regionId = odpsConf.getRegionId();

        // 优先级 1: 检查是否直接配置了 AccessKey ID 和 SecretKey
        if (!StringUtils.isNullOrWhitespaceOnly(accessId) && !StringUtils.isNullOrWhitespaceOnly(accessKey)) {
            String stsToken = odpsConf.getProperty("odps.access.security.token");

            // 检查是否存在 STS Token
            if (com.aliyun.odps.utils.StringUtils.isNotBlank(stsToken)) {
                LOG.info("Connecting to ODPS with STS Account (AccessKeyId/SecretKey/StsToken).");
                return new StsAccount(accessId, accessKey, stsToken, regionId);
            } else {
                LOG.info("Connecting to ODPS with Aliyun Account (AccessKeyId/SecretKey).");
                return new AliyunAccount(accessId, accessKey, regionId);
            }
        }
        // 优先级 2: 如果没有 AK/SK，则检查 Credentials Provider
        else if (!StringUtils.isNullOrWhitespaceOnly(odpsConf.getCredentialsProviderClass())) {
            LOG.info("AK/SK not found, trying to use credentials provider: {}", odpsConf.getCredentialsProviderClass());
            try {
                Class<?> providerClass = Class.forName(odpsConf.getCredentialsProviderClass());
                if (!ICredentialsProvider.class.isAssignableFrom(providerClass)) {
                    throw new FlinkOdpsException(
                        "Class " + odpsConf.getCredentialsProviderClass() + " does not implement "
                        + ICredentialsProvider.class.getName());
                }

                Map<String, String> params = odpsConf.getCredentialsProviderParams();
                ICredentialsProvider provider;
                if (params == null) {
                    Constructor<?> constructor = providerClass.getDeclaredConstructor();
                    provider = (ICredentialsProvider) constructor.newInstance();
                } else {
                    Constructor<?> constructor = providerClass.getDeclaredConstructor(Map.class);
                    provider = (ICredentialsProvider) constructor.newInstance(params);
                }
                return new AklessAccount(provider, regionId);
            } catch (Exception e) {
                throw new FlinkOdpsException("Constructing credentials provider failed: " + e.getMessage(), e);
            }
        }
        else {
            throw new FlinkOdpsException(
                "Failed to create ODPS account. No valid credentials provided. " +
                "Please configure either AccessKey/SecretKey (and optional STS token) or a CredentialsProvider.");
        }
    }

    public static LinkedHashMap<String, String> getPartitionSpecKVMap(PartitionSpec partitionSpec) {
        LinkedHashMap<String, String> parts = new LinkedHashMap<>(2);
        for (String k : partitionSpec.keys()) {
            parts.put(k, partitionSpec.get(k));
        }
        return parts;
    }

    public static String generatePartition(LinkedHashMap<String, String> partitionSpec) {
        checkNotNull(partitionSpec, "partitionSpec cannot be null");
        StringBuilder sb = new StringBuilder();
        String[] keys = partitionSpec.keySet().toArray(new String[0]);

        for (int i = 0; i < keys.length; ++i) {
            sb.append(keys[i]).append("='").append(partitionSpec.get(keys[i])).append("'");
            if (i + 1 < keys.length) {
                sb.append(',');
            }
        }
        return sb.toString();
    }

    public static String[] createPartitionSpec(List<Partition> odpsPartitions) {
        checkNotNull(odpsPartitions, "PartitionList cannot be null");
        String[] partitions = new String[odpsPartitions.size()];
        for (int i = 0; i < odpsPartitions.size(); i++) {
            PartitionSpec partitionSpec = odpsPartitions.get(i).getPartitionSpec();
            partitions[i] = partitionSpec.keys().stream().map(colName -> colName + "=" + partitionSpec.get(colName))
                    .collect(Collectors.joining(","));
        }
        return partitions;
    }

    public static Object convertPartitionColumn(String value, TypeInfo odpsTypeInfo) {
        switch (odpsTypeInfo.getOdpsType()) {
            case STRING:
                return value;
            case VARCHAR:
                return new Varchar(value, ((AbstractCharTypeInfo) odpsTypeInfo).getLength());
            case CHAR:
                return new Char(value, ((AbstractCharTypeInfo) odpsTypeInfo).getLength());
            case TINYINT:
                return Byte.parseByte(value);
            case SMALLINT:
                return Short.parseShort(value);
            case INT:
                return Integer.parseInt(value);
            case BIGINT:
                return Long.parseLong(value);
            default:
                throw new FlinkOdpsException("Unsupported partition column type: " + odpsTypeInfo.getTypeName());
        }
    }

    public static String objToString(@Nullable Object obj) {
        if (obj == null) {
            return null;
        }
        return obj instanceof ByteBuffer ? toHexString(((ByteBuffer) obj).array()) : obj.toString();
    }

    public static String toHexString(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    public static boolean partitionExist(OdpsMetaDataProvider tableMetaProvider, TableIdentifier identifier,
                                         String targetPartition) {
        return partitionExist(tableMetaProvider, identifier, targetPartition, false);
    }

    public static boolean partitionExist(OdpsMetaDataProvider tableMetaProvider, TableIdentifier identifier,
                                         String targetPartition, boolean refresh) {
        int attemptNum = 1;
        while (true) {
            try {
                Partition partition = tableMetaProvider.getPartition(identifier.getProject(), identifier.getSchema(),
                        identifier.getTable(), targetPartition, refresh);
                return partition != null;
            } catch (Throwable e) {
                if (attemptNum++ > 5) {
                    LOG.error("Failed to get partition: " + targetPartition + " after retrying...");
                    throw new FlinkOdpsException(e);
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    LOG.error("Failed to get partition: " + targetPartition);
                    throw new FlinkOdpsException(e);
                }
            }
        }
    }

    public static void createPartitionIfNeeded(OdpsMetaDataProvider tableMetaProvider, TableIdentifier identifier,
                                               String targetPartition) throws IOException {
        if (StringUtils.isNullOrWhitespaceOnly(targetPartition)) {
            return;
        }
        int attemptNum = 1;
        while (true) {
            try {
                Partition partition = tableMetaProvider.getPartition(identifier.getProject(), identifier.getSchema(),
                        identifier.getTable(), targetPartition, false);
                if (partition == null) {
                    Table table = tableMetaProvider.getTable(identifier.getProject(), identifier.getSchema(),
                            identifier.getTable());
                    table.createPartition(new PartitionSpec(targetPartition), true);
                    LOG.info("Create partition: " + identifier.toString() + "/" + targetPartition);
                }
                break;
            } catch (Throwable e) {
                if (attemptNum++ > 5) {
                    LOG.error("Failed to create partition: " + targetPartition + " after retrying...");
                    throw new IOException(e);
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    LOG.error("Failed to create partition: " + targetPartition);
                    throw new IOException(e);
                }
            }
        }
    }

    public static void createPartitions(Odps odps, TableIdentifier identifier, String[] targetPartition) {
        String addPartitionSQL;
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ").append(identifier.getProject()).append(".").append(identifier.getSchema()).append(".")
                .append(identifier.getTable());
        sb.append(" ADD");
        sb.append(" IF NOT EXISTS");

        for (String partition : targetPartition) {
            sb.append(" PARTITION (");
            sb.append(partition);
            sb.append(")");
        }
        sb.append(";");
        addPartitionSQL = sb.toString();

        Map<String, String> hints = new HashMap<>();
        hints.put("odps.namespace.schema", "true");
        hints.put("odps.sql.allow.namespace.schema", "true");

        int attemptNum = 1;
        while (true) {
            try {
                Instance i = SQLTask.run(odps, odps.getDefaultProject(), addPartitionSQL, hints, null);
                i.waitForSuccess();
                break;
            } catch (Throwable e) {
                if (attemptNum++ > 5) {
                    LOG.error("Failed to create partition: " + Arrays.toString(targetPartition) + " after retrying...");
                    throw new FlinkOdpsException(e);
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    LOG.error("Failed to create partition: " + Arrays.toString(targetPartition));
                    throw new FlinkOdpsException(e);
                }
            }
        }
    }

    public static void majorCompact(Odps odps, TableIdentifier identifier, String partitionName) throws IOException {
        String tableInfo = identifier.toString();
        if (!partitionName.isEmpty()) {
            PartitionSpec part = new PartitionSpec(partitionName);
            tableInfo += " partition(";
            tableInfo += part.toString();
            tableInfo += ")";
        }
        String taskName = "merge_task_" + Calendar.getInstance().getTimeInMillis();
        MergeTask task = new MergeTask(taskName, tableInfo);

        Job job = new Job();
        String guid = UUID.randomUUID().toString();
        task.setProperty("guid", guid);

        Map<String, String> hints = new HashMap<>();
        hints.put("odps.namespace.schema", "true");
        hints.put("odps.merge.task.mode", "service");
        hints.put("odps.merge.quickmerge.flag", "false");
        hints.put("odps.merge.restructure.action", "hardlink");
        hints.put("odps.merge.txn.table.compact", "major_compact");
        task.setProperty("settings", new GsonBuilder().disableHtmlEscaping().create().toJson(hints));

        job.addTask(task);
        Instance instance = null;

        try {
            instance = odps.instances().create(job);

            LOG.info("Major compact table {}, partition {}, id {}", identifier.toString(), partitionName,
                    instance.getId());

            long startTime = System.nanoTime();
            instance.waitForSuccess();

            LOG.info("Major compact table {}, partition {}, id {}, time taken ms: {}", identifier.toString(),
                    partitionName, instance.getId(), NANOSECONDS.toMillis(System.nanoTime() - startTime));
        } catch (OdpsException e) {
            if (instance != null && !instance.isTerminated()) {
                try {
                    instance.stop();
                    LOG.error("Stop compact table success, id {}", instance.getId());
                } catch (OdpsException e2) {
                    LOG.error("Stop compact failed, id {}", instance.getId(), e2);
                }
            }
            if (e.getMessage().contains("Nothing found to merge") || e.getMessage().contains(
                    "No DeltaFile found, do not need do major compaction")) {
                LOG.info("Major compact result: Nothing found to merge");
                return;
            }
            throw new IOException(e);
        }
    }

    public static TableBatchWriteSession getOrCreateWriteSession(TableIdentifier tableIdentifier, String partitionSpec,
        String sessionId, EnvironmentSettings settings, boolean useStreaming) throws IOException {
        TableWriteSessionBuilder builder = new TableWriteSessionBuilder().identifier(tableIdentifier)
                .withSessionProvider(TUNNEL_TABLE_API_PROVIDER).withSettings(settings);

        if (!StringUtils.isNullOrWhitespaceOnly(sessionId)) {
            builder.withSessionId(sessionId);
        }

        if (!StringUtils.isNullOrWhitespaceOnly(partitionSpec)) {
            builder.partition(new PartitionSpec(partitionSpec));
        }

        TableBatchWriteSession batchWriteSession;
        if (useStreaming) {
            TableStreamingWriteSession tableStreamingWriteSession = builder.buildStreamingWriteSession();
            batchWriteSession = (TableBatchWriteSession) tableStreamingWriteSession;
        } else {
            batchWriteSession = builder.buildBatchWriteSession();
        }

        if (StringUtils.isNullOrWhitespaceOnly(sessionId)) {
            LOG.info("Create session: {}, {}, {}, {}", batchWriteSession.getId(), batchWriteSession.getStatus(),
                     tableIdentifier.toString(), partitionSpec);
         }

        if (!batchWriteSession.getStatus().equals(SessionStatus.NORMAL)) {
            throw new IOException("Invalid session status: " + batchWriteSession.getStatus() + ", session id: "
                    + batchWriteSession.getId());
        }
        return batchWriteSession;
    }

    public static TableUpsertSessionImpl getOrCreateUpsertSession(TableIdentifier tableIdentifier,
                                                                  String targetPartition, String sessionId, EnvironmentSettings settings) throws IOException {
        TableWriteSessionBuilder builder = new TableWriteSessionBuilder().identifier(tableIdentifier)
                .withSessionProvider(TUNNEL_TABLE_API_PROVIDER).withSettings(settings);

        if (!StringUtils.isNullOrWhitespaceOnly(sessionId)) {
            builder.withSessionId(sessionId);
        }

        if (!StringUtils.isNullOrWhitespaceOnly(targetPartition)) {
            builder.partition(new PartitionSpec(targetPartition));
        }

        TableUpsertSessionImpl upsertSession = null;

        try {
            upsertSession = (TableUpsertSessionImpl) builder.buildUpsertSession();

            if (StringUtils.isNullOrWhitespaceOnly(sessionId)) {
                LOG.info("Create upsert session: {}, {}, {}, {}", upsertSession.getId(), upsertSession.getStatus(),
                        tableIdentifier.toString(), targetPartition);
            }

            if (!SessionStatus.NORMAL.equals(upsertSession.getStatus())) {
                throw new IOException(
                        "Invalid session status: " + upsertSession.getStatus() + ", session id: " + upsertSession.getId());
            }

        } catch (Throwable e) {
            if (upsertSession != null) {
                upsertSession.close();
            }
            throw e;
        }
        return upsertSession;
    }

    public static TableBatchWriteSession getWriteSession(TableIdentifier tableIdentifier, String partitionSpec,
                                                         String sessionId, EnvironmentSettings settings) throws IOException {
        TableWriteSessionBuilder builder = new TableWriteSessionBuilder().identifier(tableIdentifier)
                .withSessionProvider(TUNNEL_TABLE_API_PROVIDER).withSessionId(sessionId).withSettings(settings);
        if (!StringUtils.isNullOrWhitespaceOnly(partitionSpec)) {
            builder.partition(new PartitionSpec(partitionSpec));
        }

        return builder.buildBatchWriteSession();
    }

    public static TableUpsertSessionImpl getUpsertSession(TableIdentifier tableIdentifier, String targetPartition,
                                                          String sessionId, EnvironmentSettings settings) throws IOException {
        TableWriteSessionBuilder builder = new TableWriteSessionBuilder().identifier(tableIdentifier)
                .withSessionProvider(TUNNEL_TABLE_API_PROVIDER).withSessionId(sessionId).withSettings(settings);
        if (!StringUtils.isNullOrWhitespaceOnly(targetPartition)) {
            builder.partition(new PartitionSpec(targetPartition));
        }

        return (TableUpsertSessionImpl) builder.buildUpsertSession();
    }

    public static Long getLatestVersion(Odps odps, TableIdentifier identifier) throws IOException {
        try {
            Instance i =
                    SQLTask.run(odps, odps.getDefaultProject(), "select get_latest_version('" +
                                    identifier.getProject()
                                    + "." + identifier.getSchema()
                                    + "." + identifier.getTable() + "');", null,
                            null);
            i.waitForSuccess();

            List<Record> records = SQLTask.getResult(i);
            if (records != null && records.size() > 0) {
                return Long.parseLong(records.get(0).get(0).toString());
            }
            return null;
        } catch (OdpsException e) {
            throw new IOException(e);
        }
    }

    public static void validatePartitionSpec(LinkedHashMap<String, String> staticPartitionSpec,
                                             List<String> partitionKeys) {
        if (staticPartitionSpec == null || staticPartitionSpec.isEmpty()) {
            return;
        }
        List<String> unknownPartCols = staticPartitionSpec.keySet().stream().filter(k -> !partitionKeys.contains(k))
                .collect(Collectors.toList());
        Preconditions.checkArgument(unknownPartCols.isEmpty(),
                "Static partition spec contains unknown partition column: " + unknownPartCols);
        int numStaticPart = staticPartitionSpec.size();
        if (numStaticPart < partitionKeys.size()) {
            for (String partitionCol : partitionKeys) {
                if (!staticPartitionSpec.containsKey(partitionCol)) {
                    // this is a dynamic partition, make sure we have seen all static ones
                    Preconditions.checkArgument(numStaticPart == 0,
                            "Dynamic partition cannot appear before static partition");
                    return;
                } else {
                    numStaticPart--;
                }
            }
        }
    }

    public static void checkFlinkTableSchema(TableSchema odpsTableSchema,
                                             org.apache.flink.table.api.TableSchema tableSchema) {
        List<Column> odpsColumns = odpsTableSchema.getColumns();
        odpsColumns.addAll(odpsTableSchema.getPartitionColumns());
        List<String> odpsColumnNames = odpsColumns.stream().map(Column::getName).collect(Collectors.toList());

        for (String flinkColumnName : tableSchema.getFieldNames()) {
            if (!odpsColumnNames.contains(flinkColumnName)) {
                throw new IllegalArgumentException("column " + flinkColumnName + " dose not exists in odps table");
            }
        }
    }

    public static <T> T executeWithRetry(Callable<T> callable, int maxRetries, long retryDelay) throws IOException {
        int attempt = 0;
        while (true) {
            try {
                return callable.call();
            } catch (Exception e) {
                attempt++;
                if (attempt > maxRetries) {
                    throw new IOException("Failed after retries", e);
                }
                try {
                    LOG.warn("Retrying after exception: " + e.getMessage(), e);
                    Thread.sleep(retryDelay);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Retry interrupted", ie);
                }
            }
        }
    }

    public static void shutdownExecutorService(ExecutorService service) {
        if (service != null) {
            service.shutdown();

            try {
                while (!service.awaitTermination(3L, TimeUnit.SECONDS)) {
                    LOG.info("Waiting for executor service to shut down");
                }
            } catch (InterruptedException var2) {
                LOG.error("Interrupted while waiting for executor service to terminate", var2);
                Thread.currentThread().interrupt();
            }
        }
    }

    public static CompressOption compressOptionOf(String compressAlgo) {
        if (StringUtils.isNullOrWhitespaceOnly(compressAlgo)) {
            return new CompressOption();
        }
        CompressAlgorithm compressAlgorithm;
        switch (compressAlgo.toLowerCase()) {
            case "raw":
                compressAlgorithm = CompressAlgorithm.ODPS_RAW;
                break;
            case "zlib":
                compressAlgorithm = CompressAlgorithm.ODPS_ZLIB;
                break;
            case "lz4":
                compressAlgorithm = CompressAlgorithm.ODPS_LZ4_FRAME;
                break;
            case "snappy":
                compressAlgorithm = CompressAlgorithm.ODPS_SNAPPY;
                break;
            default:
                compressAlgorithm = CompressAlgorithm.ODPS_ZLIB;
        }
        return new CompressOption(compressAlgorithm, 1, 0);
    }

    public static CompressionCodec arrowCompressOptionOf(String compressAlgo) {
        return CompressionCodec.byName(compressAlgo).orElse(CompressionCodec.NO_COMPRESSION);
    }

    public static TableSchema getTableSchema(OdpsConf odpsConf, TableIdentifier identifier) {
        return getOdps(odpsConf).tables().get(identifier.getProject(), identifier.getSchema(), identifier.getTable())
                .getSchema();
    }

    public static TableSchema getTableSchema(DataSchema dataSchema) {
        Set<String> partitionKeys = new HashSet<>(dataSchema.getPartitionKeys());
        Map<Boolean, List<Column>> partitioned = dataSchema.getColumns().stream()
                .collect(Collectors.partitioningBy(
                        column -> partitionKeys.contains(column.getName())
                ));
        List<Column> dataColumns = partitioned.get(false);
        List<Column> partitionColumns = partitioned.get(true);
        TableSchema tableSchema = new TableSchema();
        tableSchema.setColumns(dataColumns);
        tableSchema.setPartitionColumns(partitionColumns);
        return tableSchema;
    }


    public static RowType getFlinkSchema(TableSchema tableSchema) {
        List<Column> columns = tableSchema.getAllColumns();
        DataType[] dataTypes = new DataType[columns.size()];
        String[] fieldNames = new String[columns.size()];

        int fieldPos = 0;
        for (Column column : columns) {
            dataTypes[fieldPos] = OdpsTypeUtil.toFlinkType(column.getTypeInfo());
            fieldNames[fieldPos] = column.getName();
            fieldPos++;
        }
        LogicalType[] types =
            Arrays.stream(dataTypes).map(DataType::getLogicalType).toArray(LogicalType[]::new);
        return RowType.of(types, fieldNames);
    }

    public static OdpsColumn[] getSerializableSelectedColumns(TableSchema tableSchema,
                                                              org.apache.flink.table.api.TableSchema flinkSchema) {
        return columnFilter(tableSchema.getColumns(), flinkSchema).stream().map(
                c -> new OdpsColumn(c.getName(), c.getTypeInfo())).toArray(OdpsColumn[]::new);
    }

    public static OdpsColumn[] getSerializableSelectedPtColumns(TableSchema tableSchema,
                                                                org.apache.flink.table.api.TableSchema flinkSchema) {
        return columnFilter(tableSchema.getPartitionColumns(), flinkSchema).stream().map(
                c -> new OdpsColumn(c.getName(), c.getTypeInfo(), true)).toArray(OdpsColumn[]::new);
    }

    public static List<String> getColumnNames(OdpsColumn[] odpsColumns) {
        return Arrays.stream(odpsColumns).map(OdpsColumn::getName).collect(Collectors.toList());
    }

    private static List<Column> columnFilter(List<Column> odpsColumns,
                                             org.apache.flink.table.api.TableSchema tableSchema) {
        List<Column> result = new ArrayList<>();
        List<String> flinkTableColumnNames = Arrays.asList(tableSchema.getFieldNames());
        for (String flinkTableColumnName : flinkTableColumnNames) {
            for (Column odpsColumn : odpsColumns) {
                if (odpsColumn.getName().equals(flinkTableColumnName)) {
                    result.add(odpsColumn);
                    break;
                }
            }
        }
        return result;
    }
}
