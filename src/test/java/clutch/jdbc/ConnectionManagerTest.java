package clutch.jdbc;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
            int connId = mgr.connect("jdbc:test:demo", "scott", "tiger",
                Map.of("role", "reporting"), 7, 11, true);
            assertEquals(1, connId);
            assertEquals(7, driver.seenLoginTimeout);
            assertEquals("scott", driver.seenProps.getProperty("user"));
            assertEquals("tiger", driver.seenProps.getProperty("password"));
            assertEquals("reporting", driver.seenProps.getProperty("role"));
            assertEquals(11_000, driver.seenNetworkTimeoutMillis);
            assertEquals(3, DriverManager.getLoginTimeout());
            mgr.disconnect(connId);
            assertTrue(driver.closed);
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
            int connId = mgr.connect("jdbc:test:demo", "scott", "tiger",
                Map.of(), null, null, false);
            assertEquals(1, connId);
            assertTrue(driver.autoCommitDisabled);
            mgr.disconnect(connId);
        } finally {
            DriverManager.deregisterDriver(driver);
        }
    }

    private static final class RecordingDriver implements Driver {
        private int seenLoginTimeout = -1;
        private Properties seenProps;
        private int seenNetworkTimeoutMillis = -1;
        private boolean autoCommitDisabled;
        private boolean closed;

        @Override
        public Connection connect(String url, Properties info) {
            if (!acceptsURL(url)) {
                return null;
            }
            seenLoginTimeout = DriverManager.getLoginTimeout();
            seenProps = new Properties();
            seenProps.putAll(info);
            closed = false;
            return (Connection) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[]{Connection.class},
                (_proxy, method, args) -> switch (method.getName()) {
                    case "setNetworkTimeout" -> {
                        seenNetworkTimeoutMillis = (Integer) args[1];
                        yield null;
                    }
                    case "setAutoCommit" -> {
                        autoCommitDisabled = !((Boolean) args[0]);
                        yield null;
                    }
                    case "isClosed" -> closed;
                    case "close" -> {
                        closed = true;
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
}
