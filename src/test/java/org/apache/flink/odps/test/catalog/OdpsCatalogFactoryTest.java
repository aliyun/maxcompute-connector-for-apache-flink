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
package org.apache.flink.odps.test.catalog;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.catalog.OdpsCatalog;
import org.apache.flink.odps.catalog.factories.OdpsCatalogFactoryOptions;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.factories.FactoryUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/** Catalog discovery/configuration without opening a remote catalog. */
public class OdpsCatalogFactoryTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void discoversCatalogAndLoadsOfflineFixture() {
        OdpsCatalog catalog = create(options());
        assertEquals("offline_project", catalog.getDefaultDatabase());
        assertEquals("testValue", catalog.getOdpsConf().getProperty("odps.test.var"));
        assertEquals("offline_catalog", catalog.getName());
    }

    @Test
    public void allowsExplicitDefaultProject() {
        Map<String, String> options = options();
        options.put(OdpsCatalogFactoryOptions.DEFAULT_PROJECT.key(), "another_offline_project");
        assertEquals("another_offline_project", create(options).getDefaultDatabase());
    }

    @Test
    public void refusesMissingConfigurationFile() throws Exception {
        Map<String, String> options = options();
        options.put(OdpsCatalogFactoryOptions.ODPS_CONF_DIR.key(), temporary.newFolder().getAbsolutePath());
        assertThrows(ValidationException.class, () -> create(options));
    }

    @Test
    public void rejectsUnknownOptions() {
        Map<String, String> options = options();
        options.put("unknown.option", "value");
        assertThrows(ValidationException.class, () -> create(options));
    }

    private Map<String, String> options() {
        Map<String, String> options = new HashMap<>();
        options.put(CommonCatalogOptions.CATALOG_TYPE.key(), OdpsCatalogFactoryOptions.IDENTIFIER);
        options.put(OdpsCatalogFactoryOptions.ODPS_CONF_DIR.key(),
                getClass().getClassLoader().getResource("test-catalog-factory-conf").getPath());
        return options;
    }

    private OdpsCatalog create(Map<String, String> options) {
        return (OdpsCatalog) FactoryUtil.createCatalog("offline_catalog", options,
                new Configuration(), getClass().getClassLoader());
    }
}
