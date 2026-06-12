package clutch.jdbc;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConnectionManagerTest {

    @Test
    void connectAppliesAndRestoresTimeouts() throws Exception {
        RecordingDriver driver = new RecordingDriver();
        int originalTimeout = DriverManager.getLoginTimeout();
        DriverManager.registerDriver(driver);
        DriverManager.setLoginTimeout(3);
        try {
            ConnectionManager mgr = new ConnectionManager();
            int connId = mgr.connect("jdbc:test:inventory", "scott", "tiger",
                Map.of("role", "reporting"), 7, 11, true, RecordingDriver.class.getName());
            assertEquals(1, connId);
            assertEquals("jdbc:test:inventory", driver.seenUrl);
            assertEquals(2, driver.connectCount);
            assertEquals(7, driver.seenLoginTimeout);
            assertEquals("scott", driver.seenProps.getProperty("user"));
            assertEquals("tiger", driver.seenProps.getProperty("password"));
            assertEquals("reporting", driver.seenProps.getProperty("role"));
            assertEquals(11_000, driver.seenNetworkTimeoutMillis);
            assertEquals(3, DriverManager.getLoginTimeout());
            mgr.disconnect(connId);
            assertEquals(2, driver.closedCount);
        } finally {
            DriverManager.deregisterDriver(driver);
            DriverManager.setLoginTimeout(originalTimeout);
        }
    }

    @Test
    void connectDisablesAutoCommitWhenRequested() throws Exception {
        RecordingDriver driver = new RecordingDriver();
        DriverManager.registerDriver(driver);
        try {
            ConnectionManager mgr = new ConnectionManager();
            int connId = mgr.connect("jdbc:test:manual-commit", "analyst", "secret",
                Map.of("applicationName", "clutch"), null, null, false,
                RecordingDriver.class.getName());
            assertEquals(1, connId);
            assertEquals("jdbc:test:manual-commit", driver.seenUrl);
            assertEquals("analyst", driver.seenProps.getProperty("user"));
            assertTrue(driver.primaryAutoCommitDisabled);
            assertTrue(driver.metadataReadOnly);
            mgr.disconnect(connId);
        } finally {
            DriverManager.deregisterDriver(driver);
        }
    }

    @Test
    void connectIgnoresUnsupportedOptionalConnectionTuning() throws Exception {
        RecordingDriver driver = new RecordingDriver();
        driver.throwOnSetAutoCommit = true;
        driver.throwOnSetReadOnly = true;
        driver.throwOnSetNetworkTimeout = true;
        DriverManager.registerDriver(driver);
        try {
            ConnectionManager mgr = new ConnectionManager();
            int connId = mgr.connect("jdbc:test:fallbacks", "svc_user", "pw",
                Map.of("module", "sync"), 5, 13, false, RecordingDriver.class.getName());
            assertEquals(1, connId);
            assertEquals("jdbc:test:fallbacks", driver.seenUrl);
            assertEquals(2, driver.connectCount);
            mgr.disconnect(connId);
            assertEquals(2, driver.closedCount);
        } finally {
            DriverManager.deregisterDriver(driver);
        }
    }

    @Test
    void connectUsesOnlyExplicitDriverClass() throws Exception {
        PoisonDriver poison = new PoisonDriver();
        RecordingDriver driver = new RecordingDriver();
        DriverManager.registerDriver(poison);
        DriverManager.registerDriver(driver);
        try {
            ConnectionManager mgr = new ConnectionManager();
            int connId = mgr.connect("jdbc:test:explicit", "svc_user", "pw",
                Map.of(), null, null, true, RecordingDriver.class.getName());

            assertEquals(1, connId);
            assertEquals(0, poison.connectCount);
            assertEquals(2, driver.connectCount);
            mgr.disconnect(connId);
        } finally {
            DriverManager.deregisterDriver(poison);
            DriverManager.deregisterDriver(driver);
        }
    }

    @Test
    void connectFailsWhenExplicitDriverClassIsNotRegistered() {
        ConnectionManager mgr = new ConnectionManager();
        SQLException error = assertThrows(SQLException.class,
            () -> mgr.connect("jdbc:test:missing", "svc_user", "pw",
                Map.of(), null, null, true, "example.MissingDriver"));

        assertTrue(error.getMessage().contains("not registered"));
    }

    private static final class RecordingDriver implements Driver {
        private String seenUrl;
        private int seenLoginTimeout = -1;
        private Properties seenProps;
        private int seenNetworkTimeoutMillis = -1;
        private boolean primaryAutoCommitDisabled;
        private boolean metadataReadOnly;
        private int connectCount;
        private int closedCount;
        private boolean throwOnSetAutoCommit;
        private boolean throwOnSetReadOnly;
        private boolean throwOnSetNetworkTimeout;

        @Override
        public Connection connect(String url, Properties info) {
            if (!acceptsURL(url)) {
                return null;
            }
            boolean metadata = connectCount++ > 0;
            seenUrl = url;
            seenLoginTimeout = DriverManager.getLoginTimeout();
            seenProps = new Properties();
            seenProps.putAll(info);
            return (Connection) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[]{Connection.class},
                (_proxy, method, args) -> switch (method.getName()) {
                    case "setNetworkTimeout" -> {
                        if (throwOnSetNetworkTimeout) {
                            throw new SQLFeatureNotSupportedException("setNetworkTimeout");
                        }
                        seenNetworkTimeoutMillis = (Integer) args[1];
                        yield null;
                    }
                    case "setAutoCommit" -> {
                        if (throwOnSetAutoCommit) {
                            throw new SQLFeatureNotSupportedException("setAutoCommit");
                        }
                        if (!metadata) {
                            primaryAutoCommitDisabled = !((Boolean) args[0]);
                        }
                        yield null;
                    }
                    case "setReadOnly" -> {
                        if (throwOnSetReadOnly) {
                            throw new SQLFeatureNotSupportedException("setReadOnly");
                        }
                        if (metadata) {
                            metadataReadOnly = (Boolean) args[0];
                        }
                        yield null;
                    }
                    case "isClosed" -> false;
                    case "close" -> {
                        closedCount++;
                        yield null;
                    }
                    case "unwrap" -> null;
                    case "isWrapperFor" -> false;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        }

        @Override
        public boolean acceptsURL(String url) {
            return url != null && url.startsWith("jdbc:test:");
        }

        @Override
        public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
            return new DriverPropertyInfo[0];
        }

        @Override
        public int getMajorVersion() {
            return 1;
        }

        @Override
        public int getMinorVersion() {
            return 0;
        }

        @Override
        public boolean jdbcCompliant() {
            return false;
        }

        @Override
        public Logger getParentLogger() {
            return Logger.getGlobal();
        }
    }

    private static final class PoisonDriver implements Driver {
        private int connectCount;

        @Override
        public Connection connect(String url, Properties info) throws SQLException {
            connectCount++;
            throw new SQLException("poison driver must not be selected");
        }

        @Override
        public boolean acceptsURL(String url) {
            return url != null && url.startsWith("jdbc:test:");
        }

        @Override
        public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
            return new DriverPropertyInfo[0];
        }

        @Override
        public int getMajorVersion() {
            return 1;
        }

        @Override
        public int getMinorVersion() {
            return 0;
        }

        @Override
        public boolean jdbcCompliant() {
            return false;
        }

        @Override
        public Logger getParentLogger() {
            return Logger.getGlobal();
        }
    }
}
