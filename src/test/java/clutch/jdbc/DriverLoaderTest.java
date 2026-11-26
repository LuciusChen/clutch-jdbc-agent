package clutch.jdbc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DriverLoaderTest {

    @Test
    void loadDriversIsolatesCorruptJarsDirectoriesAndBrokenProviders(@TempDir Path tempDir)
            throws Exception {
        // 1. Subdirectory ending in .jar must be ignored.
        Files.createDirectory(tempDir.resolve("01-directory.jar"));
        // 2. Corrupt non-ZIP jar must be skipped rather than failing the classloader.
        Files.writeString(tempDir.resolve("02-corrupt.jar"), "not a valid zip", StandardCharsets.UTF_8);
        // 3a. Valid jar with malformed META-INF/services syntax.
        writeServiceJar(tempDir.resolve("03a-malformed-service.jar"),
            "123 invalid class name!!\n");
        // 3b. Valid jar declaring a nonexistent provider and one throwing LinkageError.
        writeServiceJar(tempDir.resolve("03b-broken-provider.jar"),
            "com.example.NoSuchDriver\n" + FailingTestDriver.class.getName() + "\n");
        // 4. Healthy driver jar must still be discovered and registered.
        writeServiceJar(tempDir.resolve("04-healthy.jar"),
            HealthyTestDriver.class.getName() + "\n");

        List<Driver> registeredShims = new ArrayList<>();
        try {
            List<String> loaded = DriverLoader.loadDrivers(tempDir.toFile());
            assertTrue(loaded.contains(HealthyTestDriver.class.getName()),
                "healthy driver must still register alongside corrupt jars and broken providers");
            assertFalse(loaded.contains(FailingTestDriver.class.getName()));

            Enumeration<Driver> drivers = DriverManager.getDrivers();
            while (drivers.hasMoreElements()) {
                Driver driver = drivers.nextElement();
                if (driver instanceof DriverShim shim
                        && HealthyTestDriver.class.getName().equals(shim.delegateClassName())) {
                    registeredShims.add(driver);
                }
            }
            assertFalse(registeredShims.isEmpty(),
                "DriverManager must contain a DriverShim wrapping HealthyTestDriver");
        } finally {
            for (Driver shim : registeredShims) {
                DriverManager.deregisterDriver(shim);
            }
        }
    }

    private static void writeServiceJar(Path jarPath, String serviceEntries) throws IOException {
        try (JarOutputStream out = new JarOutputStream(Files.newOutputStream(jarPath))) {
            out.putNextEntry(new JarEntry("META-INF/services/java.sql.Driver"));
            out.write(serviceEntries.getBytes(StandardCharsets.UTF_8));
            out.closeEntry();
        }
    }

    public static final class FailingTestDriver implements Driver {
        public FailingTestDriver() {
            throw new NoClassDefFoundError("simulated incompatible driver dependency");
        }

        @Override public Connection connect(String url, Properties info) { return null; }
        @Override public boolean acceptsURL(String url) { return false; }
        @Override public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) { return new DriverPropertyInfo[0]; }
        @Override public int getMajorVersion() { return 1; }
        @Override public int getMinorVersion() { return 0; }
        @Override public boolean jdbcCompliant() { return false; }
        @Override public Logger getParentLogger() { return Logger.getGlobal(); }
    }

    public static final class HealthyTestDriver implements Driver {
        @Override public Connection connect(String url, Properties info) throws SQLException { return null; }
        @Override public boolean acceptsURL(String url) { return url != null && url.startsWith("jdbc:healthy-test:"); }
        @Override public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) { return new DriverPropertyInfo[0]; }
        @Override public int getMajorVersion() { return 1; }
        @Override public int getMinorVersion() { return 0; }
        @Override public boolean jdbcCompliant() { return false; }
        @Override public Logger getParentLogger() { return Logger.getGlobal(); }
    }
}
