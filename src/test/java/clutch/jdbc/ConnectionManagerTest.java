package clutch.jdbc;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLRecoverableException;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
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
                Map.of("role", "reporting"), 7, 11, null, true,
                RecordingDriver.class.getName());
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
                Map.of("applicationName", "clutch"), null, null, null, false,
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
    void connectFailsWhenManualCommitIsUnsupported() throws Exception {
        for (Throwable unsupported : List.of(
                new SQLFeatureNotSupportedException("setAutoCommit unsupported"),
                new AbstractMethodError("legacy setAutoCommit"))) {
            RecordingDriver driver = new RecordingDriver();
            driver.primarySetAutoCommitFailure = unsupported;
            DriverManager.registerDriver(driver);
            ConnectionManager mgr = new ConnectionManager();
            try {
                SQLException failure = assertThrows(SQLException.class,
                    () -> mgr.connect("jdbc:test:manual-commit-unsupported", "analyst", "secret",
                        Map.of(), null, null, null, false,
                        RecordingDriver.class.getName()));

                assertTrue(failure.getMessage().contains("setAutoCommit")
                    || failure.getMessage().contains("manual commit"));
                assertEquals(1, driver.connectCount,
                    "metadata connection must not open after manual-commit setup fails");
                assertEquals(1, driver.closedCount,
                    "failed primary connection must be closed");
            } finally {
                mgr.disconnectAll();
                DriverManager.deregisterDriver(driver);
            }
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
                Map.of("module", "sync"), 5, 13, null, true,
                RecordingDriver.class.getName());
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
                Map.of(), null, null, null, true, RecordingDriver.class.getName());

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
                Map.of(), null, null, null, true, "example.MissingDriver"));

        assertTrue(error.getMessage().contains("not registered"));
    }

    @Test
    void reconnectMetadataIfInvalidPreservesPrimarySession() throws Exception {
        RecordingDriver driver = new RecordingDriver();
        driver.invalidMetadataConnectionNumber = 1;
        DriverManager.registerDriver(driver);
        try {
            ConnectionManager mgr = new ConnectionManager();
            int connId = mgr.connect("jdbc:test:metadata-recovery", "reader", "secret",
                Map.of("role", "reporting"), 5, 9, null, false,
                RecordingDriver.class.getName());
            Connection primary = mgr.getPrimary(connId);
            Connection failedMetadata = mgr.getMetadata(connId);

            assertTrue(mgr.reconnectMetadataIfInvalid(connId, null));
            assertEquals(3, driver.connectCount);
            assertNotSame(failedMetadata, mgr.getMetadata(connId));
            assertSame(primary, mgr.getPrimary(connId));
            assertEquals("reader", driver.seenProps.getProperty("user"));
            assertEquals("secret", driver.seenProps.getProperty("password"));

            mgr.invalidateMetadata(connId);
            SQLException invalid = assertThrows(
                SQLException.class, () -> mgr.getMetadata(connId));
            assertTrue(invalid.getMessage().contains("Metadata connection is invalid"));
            assertSame(primary, mgr.getPrimary(connId));
            assertTrue(mgr.reconnectMetadataIfInvalid(connId, null));
            assertEquals(4, driver.connectCount);

            mgr.disconnect(connId);
            awaitClosedCount(driver, 4);
            assertEquals(4, driver.closedCount);
        } finally {
            DriverManager.deregisterDriver(driver);
        }
    }

    @Test
    void connectionFailureClassificationTraversesLinksAndRejectsNearbyCode() {
        SQLException badPacket = new SQLException(
            "ORA-12592: TNS:bad packet", "66000", 12592);
        SQLException caused = new SQLException("caused wrapper", "42000");
        caused.initCause(badPacket);
        SQLException chained = new SQLException("next wrapper", "42000");
        chained.setNextException(badPacket);
        SQLException cycle = new SQLException("cyclic wrapper", "42000", 12591) {
            @Override
            public synchronized Throwable getCause() {
                return this;
            }

            @Override
            public SQLException getNextException() {
                return this;
            }
        };

        assertTrue(ConnectionManager.isConnectionFailure(caused));
        assertTrue(ConnectionManager.isConnectionFailure(chained));
        assertFalse(ConnectionManager.isConnectionFailure(cycle));
    }

    @Test
    void poisonRemovesLogicalConnectionBeforeAsynchronousCleanup() throws Exception {
        RecordingDriver driver = new RecordingDriver();
        DriverManager.registerDriver(driver);
        try {
            ConnectionManager mgr = new ConnectionManager();
            int connId = mgr.connect("jdbc:test:poison", "reader", "secret",
                Map.of(), null, null, null, true, RecordingDriver.class.getName());

            assertTrue(mgr.hasConnection(connId));
            mgr.poison(connId);

            assertFalse(mgr.hasConnection(connId));
            SQLException error = assertThrows(SQLException.class, () -> mgr.getPrimary(connId));
            assertTrue(error.getMessage().contains("Unknown connection id"));
            awaitClosedCount(driver, 2);
            assertEquals(2, driver.closedCount);
            mgr.disconnectAll();
        } finally {
            DriverManager.deregisterDriver(driver);
        }
    }

    @Test
    void metadataDetachAndReplacementReturnBeforeBlockedCloseFinishes() throws Exception {
        for (boolean reconnect : List.of(false, true)) {
            RecordingDriver driver = new RecordingDriver();
            driver.blockMetadataClose = true;
            DriverManager.registerDriver(driver);
            ConnectionManager mgr = new ConnectionManager();
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                int connId = mgr.connect("jdbc:test:metadata-detach", "reader", "secret",
                    Map.of(), null, null, null, true, RecordingDriver.class.getName());
                Connection primary = mgr.getPrimary(connId);
                Connection oldMetadata = mgr.getMetadata(connId);

                Future<Boolean> lifecycle = executor.submit(() -> {
                    if (reconnect) {
                        return mgr.reconnectMetadataIfInvalid(connId,
                            new SQLRecoverableException("metadata transport lost", "08000"));
                    }
                    mgr.invalidateMetadata(connId);
                    return true;
                });

                assertTrue(driver.metadataCloseStarted.await(1, TimeUnit.SECONDS));
                assertTrue(lifecycle.get(200, TimeUnit.MILLISECONDS));
                assertSame(primary, mgr.getPrimary(connId));
                if (reconnect) {
                    assertNotSame(oldMetadata, mgr.getMetadata(connId));
                } else {
                    assertThrows(SQLException.class, () -> mgr.getMetadata(connId));
                }
            } finally {
                driver.releaseMetadataClose.countDown();
                executor.shutdownNow();
                mgr.disconnectAll();
                DriverManager.deregisterDriver(driver);
            }
        }
    }

    @Test
    void primaryValidationUsesSessionWallClockAndForegroundActivity() throws Exception {
        MutableClock clock = new MutableClock();
        RecordingDriver driver = new RecordingDriver();
        DriverManager.registerDriver(driver);
        try {
            ConnectionManager mgr = new ConnectionManager(clock);
            int connId = mgr.connect("jdbc:test:idle-validation", "reader", "secret",
                Map.of(), null, null, 300, false, RecordingDriver.class.getName());
            int setAutoCommitCalls = driver.primarySetAutoCommitCalls;

            clock.advanceSeconds(299);
            assertTrue(mgr.validatePrimaryIfIdle(connId, 3));
            assertEquals(0, driver.primaryValidationCalls);

            mgr.getMetadata(connId);
            clock.advanceSeconds(1);
            assertTrue(mgr.validatePrimaryIfIdle(connId, 3));
            assertEquals(1, driver.primaryValidationCalls,
                "metadata access must not refresh primary activity");
            assertEquals(3, driver.primaryValidationTimeoutSeconds);
            assertEquals(setAutoCommitCalls, driver.primarySetAutoCommitCalls);
            assertEquals(0, driver.primaryCommitCalls);
            assertEquals(0, driver.primaryRollbackCalls);

            mgr.getPrimary(connId);
            clock.advanceSeconds(299);
            assertTrue(mgr.validatePrimaryIfIdle(connId, 3));
            assertEquals(1, driver.primaryValidationCalls);
            clock.advanceSeconds(1);
            assertTrue(mgr.validatePrimaryIfIdle(connId, 3));
            assertEquals(2, driver.primaryValidationCalls);
            mgr.disconnectAll();
        } finally {
            DriverManager.deregisterDriver(driver);
        }
    }

    @Test
    void disabledPrimaryValidationNeverCallsDriver() throws Exception {
        for (Integer threshold : java.util.Arrays.asList(null, 0)) {
            MutableClock clock = new MutableClock();
            RecordingDriver driver = new RecordingDriver();
            DriverManager.registerDriver(driver);
            try {
                ConnectionManager mgr = new ConnectionManager(clock);
                int connId = mgr.connect("jdbc:test:validation-disabled", "reader", "secret",
                    Map.of(), null, null, threshold, true,
                    RecordingDriver.class.getName());

                clock.advanceSeconds(86_400);
                assertTrue(mgr.validatePrimaryIfIdle(connId, 3));
                assertEquals(0, driver.primaryValidationCalls);
                mgr.poison(connId);
                SQLException unknown = assertThrows(
                    SQLException.class, () -> mgr.validatePrimaryIfIdle(connId, 3));
                assertTrue(unknown.getMessage().contains("Unknown connection id"));
                assertEquals(0, driver.primaryValidationCalls,
                    "authoritative lookup must precede the disabled threshold check");
                mgr.disconnectAll();
            } finally {
                DriverManager.deregisterDriver(driver);
            }
        }
    }

    @Test
    void unsupportedPrimaryValidationIsSkippedButOtherErrorsPropagate() throws Exception {
        for (Throwable unsupported : List.of(
                new SQLFeatureNotSupportedException("isValid unsupported"),
                new AbstractMethodError("legacy isValid"))) {
            MutableClock clock = new MutableClock();
            RecordingDriver driver = new RecordingDriver();
            driver.primaryValidationFailure = unsupported;
            DriverManager.registerDriver(driver);
            try {
                ConnectionManager mgr = new ConnectionManager(clock);
                int connId = mgr.connect("jdbc:test:validation-unsupported", "reader", "secret",
                    Map.of(), null, null, 1, true, RecordingDriver.class.getName());
                clock.advanceSeconds(1);

                assertTrue(mgr.validatePrimaryIfIdle(connId, 3));
                assertEquals(1, driver.primaryValidationCalls);
                assertTrue(mgr.hasConnection(connId));
                mgr.disconnectAll();
            } finally {
                DriverManager.deregisterDriver(driver);
            }
        }

        MutableClock clock = new MutableClock();
        RecordingDriver driver = new RecordingDriver();
        driver.primaryValidationFailure = new SQLException("generic validation failure", "HY000");
        DriverManager.registerDriver(driver);
        try {
            ConnectionManager mgr = new ConnectionManager(clock);
            int connId = mgr.connect("jdbc:test:validation-error", "reader", "secret",
                Map.of(), null, null, 1, true, RecordingDriver.class.getName());
            clock.advanceSeconds(1);

            SQLException failure = assertThrows(
                SQLException.class, () -> mgr.validatePrimaryIfIdle(connId, 3));
            assertEquals("HY000", failure.getSQLState());
            assertTrue(mgr.hasConnection(connId),
                "ConnectionManager reports validation; Dispatcher owns poisoning");
            mgr.disconnectAll();
        } finally {
            DriverManager.deregisterDriver(driver);
        }
    }

    @Test
    void unsupportedMetadataValidationKeepsOpenSessionAfterOrdinaryError() throws Exception {
        for (Throwable unsupported : List.of(
                new SQLFeatureNotSupportedException("isValid unsupported"),
                new AbstractMethodError("legacy isValid"))) {
            RecordingDriver driver = new RecordingDriver();
            driver.metadataValidationFailure = unsupported;
            DriverManager.registerDriver(driver);
            try {
                ConnectionManager mgr = new ConnectionManager();
                int connId = mgr.connect("jdbc:test:metadata-validation-unsupported",
                    "reader", "secret", Map.of(), null, null, null, true,
                    RecordingDriver.class.getName());
                Connection metadata = mgr.getMetadata(connId);

                assertFalse(mgr.reconnectMetadataIfInvalid(connId,
                    new SQLException("ordinary metadata query error", "42000")));
                assertSame(metadata, mgr.getMetadata(connId));
                assertEquals(2, driver.connectCount);
                mgr.disconnectAll();
            } finally {
                DriverManager.deregisterDriver(driver);
            }
        }
    }

    private static void awaitClosedCount(RecordingDriver driver, int expected)
            throws InterruptedException {
        long deadline = System.nanoTime() + 1_000_000_000L;
        while (driver.closedCount < expected && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
    }

    private static final class RecordingDriver implements Driver {
        private String seenUrl;
        private int seenLoginTimeout = -1;
        private Properties seenProps;
        private int seenNetworkTimeoutMillis = -1;
        private boolean primaryAutoCommitDisabled;
        private int primarySetAutoCommitCalls;
        private int primaryCommitCalls;
        private int primaryRollbackCalls;
        private int primaryValidationCalls;
        private int primaryValidationTimeoutSeconds;
        private boolean primaryValidationResult = true;
        private Throwable primaryValidationFailure;
        private Throwable primarySetAutoCommitFailure;
        private Throwable metadataValidationFailure;
        private boolean metadataReadOnly;
        private int connectCount;
        private volatile int closedCount;
        private boolean throwOnSetAutoCommit;
        private boolean throwOnSetReadOnly;
        private boolean throwOnSetNetworkTimeout;
        private boolean blockMetadataClose;
        private final CountDownLatch metadataCloseStarted = new CountDownLatch(1);
        private final CountDownLatch releaseMetadataClose = new CountDownLatch(1);
        private int invalidMetadataConnectionNumber = -1;

        @Override
        public Connection connect(String url, Properties info) {
            if (!acceptsURL(url)) {
                return null;
            }
            int connectionNumber = connectCount++;
            boolean metadata = connectionNumber > 0;
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
                        if (!metadata && primarySetAutoCommitFailure != null) {
                            throw primarySetAutoCommitFailure;
                        }
                        if (throwOnSetAutoCommit) {
                            throw new SQLFeatureNotSupportedException("setAutoCommit");
                        }
                        if (!metadata) {
                            primarySetAutoCommitCalls++;
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
                    case "commit" -> {
                        if (!metadata) primaryCommitCalls++;
                        yield null;
                    }
                    case "rollback" -> {
                        if (!metadata) primaryRollbackCalls++;
                        yield null;
                    }
                    case "isClosed" -> false;
                    case "isValid" -> {
                        if (!metadata) {
                            primaryValidationCalls++;
                            primaryValidationTimeoutSeconds = (Integer) args[0];
                            if (primaryValidationFailure != null) {
                                throw primaryValidationFailure;
                            }
                            yield primaryValidationResult;
                        }
                        if (metadataValidationFailure != null) {
                            throw metadataValidationFailure;
                        }
                        yield connectionNumber != invalidMetadataConnectionNumber;
                    }
                    case "close" -> {
                        if (metadata && blockMetadataClose) {
                            metadataCloseStarted.countDown();
                            while (releaseMetadataClose.getCount() > 0) {
                                try {
                                    releaseMetadataClose.await();
                                } catch (InterruptedException ignored) {
                                    // Simulate a JDBC close that ignores interruption.
                                }
                            }
                        }
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

    private static final class MutableClock extends Clock {
        private Instant instant = Instant.parse("2026-01-01T00:00:00Z");

        private void advanceSeconds(long seconds) {
            instant = instant.plusSeconds(seconds);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return instant;
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
