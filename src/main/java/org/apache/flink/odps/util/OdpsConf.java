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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class OdpsConf implements Serializable {

    private final String accessId;
    private final String accessKey;
    private final String endpoint;
    private final String project;
    private String tunnelEndpoint;
    private String quotaName;
    private String regionId;
    private final Map<String, String> properties;

    private String signatureCorporation;
    private String credentialsProviderClass;
    private Map<String, String> credentialsProviderParams;

    public OdpsConf(String accessId, String accessKey, String endpoint, String project) {
        this(accessId, accessKey, endpoint, project, "");
    }

    public OdpsConf(String accessId, String accessKey, String endpoint, String project, String tunnelEndpoint) {
        this(accessId, accessKey, endpoint, project, tunnelEndpoint, "");
    }

    public OdpsConf(String accessId, String accessKey, String endpoint, String project, String tunnelEndpoint, String quotaName) {
        this.accessId = accessId;
        this.accessKey = accessKey;
        this.endpoint = endpoint;
        this.project = project;
        this.tunnelEndpoint = tunnelEndpoint;
        this.quotaName = quotaName;
        this.properties = new HashMap<>();
    }

    public void setTunnelEndpoint(String tunnelEndpoint) {
        this.tunnelEndpoint = tunnelEndpoint;
    }

    public void setCredentialsProviderClass(String credentialsProviderClass) {
        this.credentialsProviderClass = credentialsProviderClass;
    }

    public void setCredentialsProviderParams(Map<String, String> credentialsProviderParams) {
        this.credentialsProviderParams = credentialsProviderParams;
    }

    public String getTunnelEndpoint() {
        return tunnelEndpoint;
    }

    public String getAccessId() {
        return accessId;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getProject() {
        return project;
    }

    public String getQuotaName() {
        return quotaName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getCredentialsProviderClass() {
        return credentialsProviderClass;
    }

    public Map<String, String> getCredentialsProviderParams() {
        return credentialsProviderParams;
    }

    public void setRegionId(String regionId) {
        this.regionId = regionId;
    }

    public String getRegionId() {
        return regionId;
    }

    public void setSignatureCorporation(String signatureCorporation) {
        this.signatureCorporation = signatureCorporation;
    }

    public String getSignatureCorporation() {
        return signatureCorporation;
    }

    public void setProperty(String key, String value) {
        this.properties.put(key, value);
    }

    public boolean containsProperty(String key) {
        return this.properties.containsKey(key);
    }

    public String getProperty(String key) {
        return this.properties.get(key);
    }

    public String getPropertyOrDefault(String key, String defaultValue) {
        return this.properties.getOrDefault(key, defaultValue);
    }

    public int getPropertyOrDefault(String key, int defaultValue) {
        if (this.properties.containsKey(key)) {
            return Integer.parseInt(getProperty(key));
        } else {
            return defaultValue;
        }
    }

    public Boolean getPropertyOrDefault(String key, boolean defaultValue) {
        if (this.properties.containsKey(key)) {
            return Boolean.parseBoolean(getProperty(key));
        } else {
            return defaultValue;
        }
    }

    @Override
    public String toString() {
        return "OdpsConf {" +
                "accessId='" + accessId + '\'' +
                ", accessKey='" + accessKey + '\'' +
                ", endpoint='" + endpoint + '\'' +
                ", project='" + project + '\'' +
                ", tunnelEndpoint='" + tunnelEndpoint + '\'' +
                ", properties='" + properties.toString() + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OdpsConf that = (OdpsConf) o;
        return project.equals(that.project)
                && accessId.equals(that.accessId)
                && accessKey.equals(that.accessKey)
                && endpoint.equals(that.endpoint)
                && tunnelEndpoint.equals(that.tunnelEndpoint)
                && Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                project,
                accessId,
                accessKey,
                endpoint,
                tunnelEndpoint,
                properties);
    }
}
