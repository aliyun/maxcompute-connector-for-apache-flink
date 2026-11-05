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

package org.apache.flink.odps.sink.table;

import java.util.concurrent.TimeUnit;

import com.aliyun.odps.Odps;
import com.aliyun.odps.ProxyConfig;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.table.configuration.RestOptions;
import com.aliyun.odps.table.enviroment.Credentials;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.enviroment.ExecutionEnvironment;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.tunnel.io.TunnelRetryHandler.RetryPolicy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.table.OdpsOptions;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TableUtils {

    private static final Logger LOG = LoggerFactory.getLogger(TableUtils.class);

    public static class RetryLogger extends RestClient.RetryLogger {
        private static final Logger LOG = LoggerFactory.getLogger(RetryLogger.class);

        @Override
        public void onRetryLog(Throwable e, long retryCount, long retrySleepTime) {
            // Log the exception and retry details
            LOG.warn(
                "Retry attempt #" + retryCount + " failed. " +
                "Exception: " + e.getMessage() + ". " +
                "Sleeping for " + retrySleepTime + "ms before next attempt."
            );
        }
    }

    // retry 3 times and wait 5 seconds for each retry
    public static class FlinkDefaultRetryPolicy implements RetryPolicy {
        @Override
        public boolean shouldRetry(Exception e, int attempt) {
            return attempt <= 3;
        }
        @Override
        public long getRetryWaitTime(int attempt) {
            return TimeUnit.SECONDS.toMillis(5);
        }
    }

    public static TableTunnel getTableTunnel(EnvironmentSettings settings) {
        Odps odps = getOdps(settings);
        com.aliyun.odps.tunnel.Configuration configuration = com.aliyun.odps.tunnel.Configuration.builder(odps)
            .withRetryLogger(new RetryLogger())
            .withRetryPolicy(new FlinkDefaultRetryPolicy())
            // set compress option to zlib, since the default compress option change to lz4_frame
            .withCompressOptions(new CompressOption())
            .build();

        TableTunnel tunnel = new TableTunnel(odps, configuration);
        if (settings.getTunnelEndpoint().isPresent()) {
            tunnel.setEndpoint(settings.getTunnelEndpoint().get());
        }
        if (settings.getQuotaName().isPresent()) {
            tunnel.getConfig().setQuotaName(settings.getQuotaName().get());
        }
        return tunnel;
    }

    public static Odps getOdps(EnvironmentSettings settings) {
        Odps odps = ExecutionEnvironment.create(settings).createOdpsClient();
        // load fron env by default
        ProxyConfig.Builder builder = new ProxyConfig.Builder().loadFromEnvironment();
        // customized proxy can cover the default config
        settings.getHttpProxy().ifPresent(builder::withHttpProxy);
        settings.getHttpsProxy().ifPresent(builder::withHttpsProxy);
        builder.withDisableNettyLocalResolver(settings.isDisableNettyLocalResolver());
        odps.options().setProxyConfig(builder.build());
        LOG.info("final http proxy: {}, https proxy: {}, socks proxy: {}, disableLocalResolver“: {}",
                odps.options().getProxyConfig().getProxy(ProxyConfig.Type.HTTP)
                        .map(p -> p.address().toString()).orElse("empty"),
                odps.options().getProxyConfig().getProxy(ProxyConfig.Type.HTTPS)
                        .map(p -> p.address().toString()).orElse("empty"),
                odps.options().getProxyConfig().getProxy(ProxyConfig.Type.SOCKS5)
                        .map(p -> p.address().toString()).orElse("empty"),
                settings.isDisableNettyLocalResolver());
        return odps;
    }

    public static EnvironmentSettings getEnvironmentSettings(OdpsConf odpsConf) {
        Account account = OdpsUtils.getDefaultAccount(odpsConf);
        Credentials credentials = Credentials.newBuilder().withAccount(account).build();
        EnvironmentSettings.Builder builder = EnvironmentSettings.newBuilder()
                .inAutoMode()
                .withCredentials(credentials)
                .withServiceEndpoint(odpsConf.getEndpoint());
        if (!StringUtils.isNullOrWhitespaceOnly(odpsConf.getTunnelEndpoint())) {
            builder.withTunnelEndpoint(odpsConf.getTunnelEndpoint());
        }
        if (!StringUtils.isNullOrWhitespaceOnly(odpsConf.getQuotaName())) {
            builder.withQuotaName(odpsConf.getQuotaName());
        }
        return builder.build();
    }

    public static EnvironmentSettings getEnvironmentSettings(Odps odps, String tunnelEndpoint, Configuration configuration) {
        Credentials credentials = Credentials.newBuilder()
                .withAccount(odps.getAccount())
                .withAppAccount(odps.getAppAccount())
                .withAppStsAccount(odps.getAppStsAccount())
                .build();
        EnvironmentSettings.Builder env = EnvironmentSettings.newBuilder()
                .inAutoMode()
                .withDefaultProject(odps.getDefaultProject())
                .withServiceEndpoint(odps.getEndpoint())
                .withCredentials(credentials);
        if (!StringUtils.isNullOrWhitespaceOnly(tunnelEndpoint)) {
            env.withTunnelEndpoint(tunnelEndpoint);
        }

        String quotaName = configuration.getString(OdpsOptions.TUNNEL_QUOTA_NAME);
        if (!StringUtils.isNullOrWhitespaceOnly(quotaName)) {
            LOG.info("set quota name: {}", quotaName);
            env.withQuotaName(quotaName);
        }

        env.withSlotNum((long) configuration.getInteger(OdpsOptions.UPSERT_WRITE_SLOT_NUM));
        env.withRestOptions(RestOptions.newBuilder()
                .witUserAgent("Flink-maxcompute-connector")
                .withReadTimeout(Math.toIntExact(Math.max(1L, configuration.get(OdpsOptions.READ_TIMEOUT).getSeconds())))
                .withAsyncTimeoutInSeconds(configuration.getInteger(OdpsOptions.UPSERT_COMMIT_TIMEOUT))
                .withUpsertConcurrentNum(configuration.getInteger(OdpsOptions.UPSERT_FLUSH_CONCURRENT))
                .withUpsertNetworkNum(configuration.getInteger(OdpsOptions.UPSERT_NIO_THREAD_NUM))
                .build());
        if (!StringUtils.isNullOrWhitespaceOnly(configuration.getString(OdpsOptions.HTTP_PROXY))) {
            env.withHttpProxy(configuration.getString(OdpsOptions.HTTP_PROXY));
        }
        if (!StringUtils.isNullOrWhitespaceOnly(configuration.getString(OdpsOptions.HTTPS_PROXY))) {
            env.withHttpsProxy(configuration.getString(OdpsOptions.HTTPS_PROXY));
        }
        env.withDisableNettyLocalResolver(configuration.getBoolean(OdpsOptions.ODPS_PROXY_DISABLE_RESOLVER));
        return env.build();
    }
}
