package clutch.jdbc;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/**
 * Loads JDBC drivers from the drivers/ directory at runtime.
 *
 * Why external jars?
 * - Oracle ojdbc cannot be redistributed; user must supply it.
 * - Keeps the agent jar small and independent of driver versions.
 *
 * Why DriverShim?
 * - DriverManager refuses drivers loaded by a non-system classloader
 *   (it checks that the driver's classloader is an ancestor of the caller's).
 * - DriverShim wraps the externally-loaded driver and registers it under
 *   the system classloader, bypassing this restriction.
 */
public class DriverLoader {

    private static final System.Logger LOG = System.getLogger(DriverLoader.class.getName());

    /**
     * Scan driversDir for *.jar files, load each one, and register all
     * JDBC drivers found via ServiceLoader<Driver>.
     *
     * @param driversDir path to the drivers/ directory
     * @return list of loaded driver class names (for diagnostics)
     */
    public static List<String> loadDrivers(File driversDir) {
        List<String> loaded = new ArrayList<>();

        if (!driversDir.isDirectory()) {
            LOG.log(System.Logger.Level.WARNING,
                    "drivers directory not found: {0}", driversDir.getAbsolutePath());
            return loaded;
        }

        File[] jars = driversDir.listFiles(f -> f.getName().endsWith(".jar"));
        if (jars == null || jars.length == 0) {
            LOG.log(System.Logger.Level.INFO, "No driver jars found in {0}",
                    driversDir.getAbsolutePath());
            return loaded;
        }

        URL[] urls = new URL[jars.length];
        try {
            for (int i = 0; i < jars.length; i++) {
                urls[i] = jars[i].toURI().toURL();
                LOG.log(System.Logger.Level.INFO, "Found driver jar: {0}", jars[i].getName());
            }
        } catch (Exception e) {
            LOG.log(System.Logger.Level.ERROR, "Failed to build driver URLs", e);
            return loaded;
        }

        URLClassLoader driverCL = new URLClassLoader(urls, ClassLoader.getSystemClassLoader());

        // ServiceLoader discovers Driver implementations declared in
        // META-INF/services/java.sql.Driver inside each jar.
        ServiceLoader<Driver> sl = ServiceLoader.load(Driver.class, driverCL);
        for (Driver driver : sl) {
            try {
                // Wrap in DriverShim so DriverManager accepts it.
                DriverManager.registerDriver(new DriverShim(driver));
                loaded.add(driver.getClass().getName());
                LOG.log(System.Logger.Level.INFO, "Registered driver: {0}",
                        driver.getClass().getName());
            } catch (Exception e) {
                LOG.log(System.Logger.Level.WARNING,
                        "Failed to register driver {0}: {1}",
                        driver.getClass().getName(), e.getMessage());
            }
        }

        return loaded;
    }
}
