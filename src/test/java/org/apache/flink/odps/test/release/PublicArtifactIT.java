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
package org.apache.flink.odps.test.release;

import org.junit.Test;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import java.util.ServiceLoader;
import java.util.zip.ZipFile;

import static org.junit.Assert.*;

/** Runs the distributed jar with only Flink's provided dependencies, never target/classes. */
public class PublicArtifactIT {
    @Test
    public void distributedJarMatchesItsRuntimeAndContainsNoFlinkFrameworkClasses() throws Exception {
        File jar = new File(System.getProperty("connector.shaded.jar"));
        try (ZipFile zip = new ZipFile(jar); URLClassLoader loader = consumerLoader(jar)) {
            java.util.Properties version = new java.util.Properties();
            try (java.io.InputStream in = zip.getInputStream(zip.getEntry("version.properties"))) {
                version.load(in);
            }
            String runtime = (String) loader.loadClass("org.apache.flink.runtime.util.EnvironmentInformation")
                    .getMethod("getVersion").invoke(null);
            assertEquals(version.getProperty("flink.version"), runtime);
            assertTrue(runtime.startsWith(version.getProperty("flink.line") + "."));
            assertTrue(jar.getName().startsWith("maxcompute-connector-for-apache-flink-"
                    + version.getProperty("flink.line").replace('.', '-') + "-"));
            assertNotNull(zip.getEntry("org/apache/flink/odps/storage/StorageWriter.class"));
            assertNotNull(zip.getEntry("org/apache/flink/odps/sink/shuffle/AssignmentComputer.class"));
            assertNotNull(zip.getEntry("org/apache/flink/odps/shim/FlinkSinkAdapter.class"));
            for (java.util.zip.ZipEntry entry : java.util.Collections.list(zip.entries())) {
                String name = entry.getName();
                assertFalse("Flink framework must be provided by the matching cluster: " + name,
                        name.startsWith("org/apache/flink/") && name.endsWith(".class")
                                && !name.startsWith("org/apache/flink/odps/"));
            }
        }
    }

    @Test
    public void shadedJarRetainsRelocatedServiceProvidersAndLegalFiles() throws Exception {
        File jar = new File(System.getProperty("connector.shaded.jar"));
        assertTrue("shaded artifact must exist", jar.isFile());
        try (URLClassLoader loader = consumerLoader(jar)) {
            List<String> factories = providers(loader, "org.apache.flink.table.factories.Factory");
            assertTrue(factories.contains("org.apache.flink.odps.catalog.factories.OdpsCatalogFactory"));
            assertTrue(factories.contains("org.apache.flink.odps.table.factories.OdpsDynamicTableFactory"));
            List<String> sessions = providers(loader,
                    "flink.connector.odps.com.aliyun.odps.table.Session$Provider");
            assertTrue(sessions.contains("org.apache.flink.odps.sink.table.TunnelWriteSessionProviderImpl"));
            assertEquals("all SDK session providers must survive relocation", 3, sessions.size());
            assertFalse(providers(loader,
                    "flink.connector.odps.com.fasterxml.jackson.databind.Module").isEmpty());
            assertFalse(providers(loader,
                    "flink.connector.odps.com.fasterxml.jackson.core.JsonFactory").isEmpty());
            assertFalse(providers(loader,
                    "flink.connector.odps.com.fasterxml.jackson.core.ObjectCodec").isEmpty());
            try {
                loader.loadClass("com.aliyun.odps.Odps");
                fail("consumer must not accidentally use the unshaded SDK from the test classpath");
            } catch (ClassNotFoundException expected) {
                // Only the relocated SDK is part of the consumer's dependency closure.
            }
        }
        try (ZipFile zip = new ZipFile(jar)) {
            assertNotNull(zip.getEntry("META-INF/LICENSE"));
            assertNotNull(zip.getEntry("META-INF/NOTICE"));
            assertNotNull(zip.getEntry("META-INF/licenses/third-party.json"));
        }
    }

    @Test
    public void sdkXmlTransportWorksWithJdkStax() throws Exception {
        try (URLClassLoader loader = consumerLoader(new File(System.getProperty("connector.shaded.jar")))) {
            Class<?> type = loader.loadClass(
                    "flink.connector.odps.com.aliyun.odps.simpleframework.xml.core.Persister");
            Object serializer = type.getConstructor().newInstance();
            Object value = type.getMethod("read", Class.class, String.class)
                    .invoke(serializer, String.class, "<string>offline &amp; transport</string>");
            assertEquals("offline & transport", value);
            assertThrows(ClassNotFoundException.class, () -> loader.loadClass("com.bea.xml.stream.EventFactory"));
        }
    }

    @Test
    public void sdkSqlReadbackParsesQuotedCsvInDistributedJar() throws Exception {
        try (URLClassLoader loader = consumerLoader(new File(System.getProperty("connector.shaded.jar")))) {
            Class<?> type = loader.loadClass("flink.connector.odps.com.aliyun.odps.utils.CSVRecordParser");
            Object result = type.getMethod("parse", String.class)
                    .invoke(null, "id,value\n1,\"quoted,value\"\n");
            List<?> records = (List<?>) result.getClass().getMethod("getRecords").invoke(result);
            assertEquals(1, records.size());
            Object record = records.get(0);
            assertEquals("quoted,value", record.getClass().getMethod("getString", int.class).invoke(record, 1));
        }
    }

    @Test
    public void legalCatalogMatchesExactBundledDependencyClosure() throws Exception {
        JsonObject tree = JsonParser.parseString(new String(Files.readAllBytes(Paths.get(
                System.getProperty("connector.dependency.tree"))), StandardCharsets.UTF_8)).getAsJsonObject();
        Set<String> expected = new HashSet<>();
        collectBundled(tree, expected);
        try (ZipFile jar = new ZipFile(System.getProperty("connector.shaded.jar"))) {
            JsonObject catalog;
            try (java.io.Reader reader = new java.io.InputStreamReader(jar.getInputStream(
                    jar.getEntry("META-INF/licenses/third-party.json")), StandardCharsets.UTF_8)) {
                catalog = JsonParser.parseReader(reader).getAsJsonObject();
            }
            Set<String> actual = new HashSet<>();
            for (JsonElement element : catalog.getAsJsonArray("components")) {
                JsonObject component = element.getAsJsonObject();
                actual.add(component.get("coordinate").getAsString() + ":"
                        + component.get("classifier").getAsString());
                assertFalse("every bundled dependency needs legal material",
                        component.getAsJsonArray("files").size() == 0);
                for (JsonElement file : component.getAsJsonArray("files")) {
                    assertNotNull("retained legal file must be in the artifact: " + file.getAsString(),
                            jar.getEntry("META-INF/licenses/" + file.getAsString()));
                }
            }
            assertEquals("regenerate/review legal material when dependencies change", expected, actual);
        }
    }

    private static void collectBundled(JsonObject node, Set<String> coordinates) {
        if (!node.has("children")) { return; }
        for (JsonElement element : node.getAsJsonArray("children")) {
            JsonObject child = element.getAsJsonObject();
            String scope = child.get("scope").getAsString();
            if (scope.equals("compile") || scope.equals("runtime")) {
                coordinates.add(child.get("groupId").getAsString() + ":"
                        + child.get("artifactId").getAsString() + ":"
                        + child.get("version").getAsString() + ":"
                        + child.get("classifier").getAsString());
            }
            collectBundled(child, coordinates);
        }
    }

    private static URLClassLoader consumerLoader(File jar) throws Exception {
        List<URL> urls = new ArrayList<>();
        urls.add(jar.toURI().toURL());
        String classpath = new String(Files.readAllBytes(Paths.get(
                System.getProperty("connector.provided.classpath"))), StandardCharsets.UTF_8).trim();
        for (String path : classpath.split(File.pathSeparator)) {
            urls.add(new File(path).toURI().toURL());
        }
        return new URLClassLoader(urls.toArray(new URL[0]), ClassLoader.getSystemClassLoader().getParent());
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static List<String> providers(ClassLoader loader, String name) throws Exception {
        List<String> names = new ArrayList<>();
        Class type = Class.forName(name, true, loader);
        for (Object provider : ServiceLoader.load(type, loader)) {
            names.add(provider.getClass().getName());
        }
        assertFalse("SPI must expose providers: " + name, names.isEmpty());
        return names;
    }
}
