package clutch.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.Savepoint;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLRecoverableException;
import java.sql.SQLException;
import java.time.Clock;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Maintains a map of connId → logical JDBC session.
 * Each session owns a primary connection for foreground queries and a separate
 * metadata connection for schema/object introspection.
 */
public class ConnectionManager {

    private static final System.Logger LOG = System.getLogger(ConnectionManager.class.getName());

    private final Clock clock;
    private final AtomicInteger nextId = new AtomicInteger(1);
    private final AtomicInteger nextSavepointId = new AtomicInteger(1);
    private final Map<Integer, Session> connections = new ConcurrentHashMap<>();
    private final ExecutorService networkTimeoutExecutor = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "clutch-jdbc-network-timeout");
        t.setDaemon(true);
        return t;
    });

    /**
     * Products whose schema-wide listings run on a third, lazily opened session
     * so they cannot hold the metadata session for seconds while an interactive
     * lookup waits.  Only Oracle has shown that cost; other products keep two
     * sessions and never pay a third logon.
     */
    private final Predicate<String> bulkProducts;

    /** Create a connection manager using the system wall clock. */
    public ConnectionManager() {
        this(Clock.systemUTC());
    }

    ConnectionManager(Clock clock) {
        this(clock, ConnectionManager::isOracleProduct);
    }

    ConnectionManager(Clock clock, Predicate<String> bulkProducts) {
        this.clock = Objects.requireNonNull(clock);
        this.bulkProducts = Objects.requireNonNull(bulkProducts);
    }

    private static boolean isOracleProduct(String productName) {
        return productName != null
            && productName.toLowerCase(Locale.ROOT).contains("oracle");
    }

    private String productName(Connection connection) {
        try {
            return connection.getMetaData().getDatabaseProductName();
        } catch (SQLException | RuntimeException e) {
            return null;
        }
    }

    /**
     * Open a new JDBC connection and return its assigned connId.
     *
     * @param url      full JDBC URL (e.g. "jdbc:oracle:thin:@//host:1521/ORCL")
     * @param user     database user (may be null for URL-embedded credentials)
     * @param password database password (may be null)
     * @param props    extra driver properties (e.g. oracle.net.tns_admin)
     * @param validateAfterIdleSeconds validate the primary connection after this
     *        many idle seconds; null or zero disables validation
     */
    public int connect(String url, String user, String password, Map<String, String> props,
                       Integer connectTimeoutSeconds, Integer networkTimeoutSeconds,
                       Integer validateAfterIdleSeconds,
                       boolean autoCommit, String driverClass)
            throws SQLException {
        long validateAfterIdleMillis = validationIdleMillis(validateAfterIdleSeconds);
        Properties p = new Properties();
        if (props != null) p.putAll(props);
        if (user != null)     p.setProperty("user",     user);
        if (password != null) p.setProperty("password", password);

        Connection primary = openConnection(url, p, connectTimeoutSeconds, driverClass);
        try {
            configurePrimaryConnection(primary, autoCommit, networkTimeoutSeconds);
            Connection metadata = openConnection(url, p, connectTimeoutSeconds, driverClass);
            try {
                configureMetadataConnection(metadata, networkTimeoutSeconds);
                int id = nextId.getAndIncrement();
                connections.put(id, new Session(
                    primary, metadata, url, p, connectTimeoutSeconds,
                    networkTimeoutSeconds, driverClass,
                    validateAfterIdleMillis, clock.millis(),
                    bulkProducts.test(productName(primary))));
                return id;
            } catch (SQLException | RuntimeException e) {
                closeQuietly(metadata);
                throw e;
            }
        } catch (SQLException | RuntimeException e) {
            closeQuietly(primary);
            throw e;
        }
    }

    private long validationIdleMillis(Integer validateAfterIdleSeconds) {
        if (validateAfterIdleSeconds == null || validateAfterIdleSeconds == 0) {
            return 0L;
        }
        if (validateAfterIdleSeconds < 0) {
            throw new IllegalArgumentException(
                "validate-after-idle-seconds must be non-negative");
        }
        return validateAfterIdleSeconds * 1_000L;
    }

    private void configurePrimaryConnection(Connection conn, boolean autoCommit,
                                            Integer networkTimeoutSeconds)
            throws SQLException {
        if (!autoCommit) {
            try {
                conn.setAutoCommit(false);
            } catch (AbstractMethodError e) {
                throw new SQLException("JDBC driver does not support manual commit mode", e);
            }
        }
        applyNetworkTimeout(conn, networkTimeoutSeconds);
    }

    private void configureMetadataConnection(Connection conn, Integer networkTimeoutSeconds)
            throws SQLException {
        try {
            conn.setAutoCommit(true);
        } catch (SQLFeatureNotSupportedException | AbstractMethodError e) {
            logUnsupportedCapability("setAutoCommit(true)", e);
        }
        try {
            conn.setReadOnly(true);
        } catch (SQLFeatureNotSupportedException | AbstractMethodError e) {
            logUnsupportedCapability("setReadOnly(true)", e);
        }
        applyNetworkTimeout(conn, networkTimeoutSeconds);
    }

    private void applyNetworkTimeout(Connection conn, Integer networkTimeoutSeconds)
            throws SQLException {
        if (networkTimeoutSeconds != null && networkTimeoutSeconds > 0) {
            try {
                conn.setNetworkTimeout(networkTimeoutExecutor, networkTimeoutSeconds * 1000);
            } catch (SQLFeatureNotSupportedException | AbstractMethodError e) {
                logUnsupportedCapability("setNetworkTimeout(" + networkTimeoutSeconds + "s)", e);
            }
        }
    }

    private void logUnsupportedCapability(String capability, Throwable error) {
        LOG.log(System.Logger.Level.DEBUG,
            "Driver does not support optional JDBC capability: " + capability, error);
    }

    private Connection openConnection(String url, Properties props, Integer connectTimeoutSeconds,
                                      String driverClass)
            throws SQLException {
        if (connectTimeoutSeconds == null || connectTimeoutSeconds <= 0) {
            return connectWithSelectedDriver(url, props, driverClass);
        }
        synchronized (DriverManager.class) {
            int previousTimeout = DriverManager.getLoginTimeout();
            DriverManager.setLoginTimeout(connectTimeoutSeconds);
            try {
                return connectWithSelectedDriver(url, props, driverClass);
            } finally {
                DriverManager.setLoginTimeout(previousTimeout);
            }
        }
    }

    private Connection connectWithSelectedDriver(String url, Properties props, String driverClass)
            throws SQLException {
        if (driverClass == null || driverClass.isBlank()) {
            throw new SQLException("JDBC driver class is required");
        }

        Enumeration<Driver> drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            Driver driver = drivers.nextElement();
            if (driverClass.equals(driverClassName(driver))) {
                Connection connection = driver.connect(url, props);
                if (connection != null) {
                    return connection;
                }
                throw new SQLException("JDBC driver " + driverClass + " does not accept URL: " + url);
            }
        }
        throw new SQLException("JDBC driver class not registered: " + driverClass);
    }

    private String driverClassName(Driver driver) {
        if (driver instanceof DriverShim shim) {
            return shim.delegateClassName();
        }
        return driver.getClass().getName();
    }

    /** Return the live primary Connection for {@code connId}, or throw if unknown. */
    public Connection getPrimary(int connId) throws SQLException {
        Session session = requireSession(connId);
        session.markPrimaryUsed(clock.millis());
        return session.primary();
    }

    /** Commit the primary transaction and invalidate its savepoint handles. */
    public void commit(int connId) throws SQLException {
        Session session = requireSession(connId);
        synchronized (session) {
            try {
                session.primary().commit();
                session.savepoints.clear();
            } finally {
                session.markPrimaryUsed(clock.millis());
            }
        }
    }

    /** Roll back the primary transaction and invalidate its savepoint handles. */
    public void rollback(int connId) throws SQLException {
        Session session = requireSession(connId);
        synchronized (session) {
            try {
                session.primary().rollback();
                session.savepoints.clear();
            } finally {
                session.markPrimaryUsed(clock.millis());
            }
        }
    }

    /** Set primary auto-commit and invalidate savepoint handles after success. */
    public void setAutoCommit(int connId, boolean autoCommit) throws SQLException {
        Session session = requireSession(connId);
        synchronized (session) {
            try {
                Connection primary = session.primary();
                if (primary.getAutoCommit() != autoCommit) {
                    primary.setAutoCommit(autoCommit);
                    session.savepoints.clear();
                }
            } finally {
                session.markPrimaryUsed(clock.millis());
            }
        }
    }

    /**
     * Create a savepoint on a manual-commit primary connection.
     *
     * @return an opaque process-local identifier for later rollback or release
     */
    public int createSavepoint(int connId) throws SQLException {
        Session session = requireSession(connId);
        synchronized (session) {
            Connection primary = session.primary();
            if (primary.getAutoCommit()) {
                throw new SQLException("Cannot create a savepoint in auto-commit mode");
            }
            if (!primary.getMetaData().supportsSavepoints()) {
                throw new SQLFeatureNotSupportedException(
                    "JDBC driver does not support savepoints");
            }
            Savepoint savepoint = primary.setSavepoint();
            int savepointId = nextSavepointId.getAndIncrement();
            session.savepoints.put(savepointId, savepoint);
            session.markPrimaryUsed(clock.millis());
            return savepointId;
        }
    }

    /**
     * Roll back to and release the savepoint identified by {@code savepointId}.
     * Savepoints created after the target are invalid once rollback succeeds.
     */
    public void rollbackSavepoint(int connId, int savepointId) throws SQLException {
        Session session = requireSession(connId);
        synchronized (session) {
            Savepoint savepoint = session.savepoint(savepointId);
            try {
                session.primary().rollback(savepoint);
                session.savepoints.keySet().removeIf(id -> id > savepointId);
                releaseSavepointIfSupported(session.primary(), savepoint);
            } finally {
                session.savepoints.remove(savepointId);
                session.markPrimaryUsed(clock.millis());
            }
        }
    }

    /** Release {@code savepointId} and every savepoint created after it. */
    public void releaseSavepoint(int connId, int savepointId) throws SQLException {
        Session session = requireSession(connId);
        synchronized (session) {
            Savepoint savepoint = session.savepoint(savepointId);
            releaseSavepointIfSupported(session.primary(), savepoint);
            session.savepoints.keySet().removeIf(id -> id >= savepointId);
            session.markPrimaryUsed(clock.millis());
        }
    }

    /**
     * Release {@code savepoint} when the driver implements optional release.
     * A transaction boundary remains valid when JDBC reports that only explicit
     * release is unsupported; commit or rollback will discard the database-side
     * savepoint.
     */
    private void releaseSavepointIfSupported(Connection connection, Savepoint savepoint)
            throws SQLException {
        try {
            connection.releaseSavepoint(savepoint);
        } catch (SQLFeatureNotSupportedException unsupported) {
            LOG.log(System.Logger.Level.DEBUG,
                "JDBC driver does not support explicit savepoint release");
        }
    }

    private Session requireSession(int connId) throws SQLException {
        Session session = connections.get(connId);
        if (session == null) {
            throw new SQLException("Unknown connection id: " + connId);
        }
        return session;
    }

    /** Mark successful or attempted foreground use of the primary JDBC session. */
    public void markPrimaryUsed(int connId) {
        Session session = connections.get(connId);
        if (session != null) {
            session.markPrimaryUsed(clock.millis());
        }
    }

    /**
     * Validate the primary connection only after its configured idle interval.
     * A missing session is reported before the disabled/idle checks so callers
     * can authoritatively report that no user SQL was started.
     */
    public boolean validatePrimaryIfIdle(int connId, int timeoutSeconds)
            throws SQLException {
        Session session = requireSession(connId);
        long thresholdMillis = session.validateAfterIdleMillis;
        long idleMillis = Math.max(0L, clock.millis() - session.lastPrimaryUseMillis);
        if (thresholdMillis == 0L || idleMillis < thresholdMillis) {
            return true;
        }
        try {
            return session.primary().isValid(timeoutSeconds);
        } catch (SQLFeatureNotSupportedException | AbstractMethodError unsupported) {
            logUnsupportedCapability("isValid(" + timeoutSeconds + "s)", unsupported);
            return true;
        }
    }

    /** Mark use of the metadata or bulk session of {@code connId}, for idle validation. */
    public void markSessionUsed(int connId, CursorManager.Lane lane) {
        Session session = connections.get(connId);
        if (session != null) {
            session.markUsed(lane, clock.millis());
        }
    }

    /**
     * Return whether the metadata or bulk session of {@code connId} sat idle
     * past the validation interval and then failed {@code isValid}.  A NAT or
     * firewall that drops an idle connection leaves its socket silent, so a
     * request on it waits out the network timeout, and Clutch, whose request
     * timeout is no longer, retires the whole logical connection first.
     */
    public boolean idleSessionDead(int connId, CursorManager.Lane lane, int timeoutSeconds) {
        Session session = connections.get(connId);
        if (session == null || session.validateAfterIdleMillis == 0L) {
            return false;
        }
        Connection connection = lane == CursorManager.Lane.BULK
            ? session.bulk() : session.metadata();
        if (connection == null
            || clock.millis() - session.lastUseMillis(lane) < session.validateAfterIdleMillis) {
            return false;
        }
        try {
            return !connection.isValid(timeoutSeconds);
        } catch (SQLFeatureNotSupportedException | AbstractMethodError unsupported) {
            logUnsupportedCapability("isValid(" + timeoutSeconds + "s)", unsupported);
            return false;
        } catch (SQLException e) {
            return true;
        }
    }

    /** Return whether {@code connId} still names a live logical session. */
    public boolean hasConnection(int connId) {
        return connections.containsKey(connId);
    }

    /** Return the live metadata Connection for {@code connId}, or throw if unknown. */
    public Connection getMetadata(int connId) throws SQLException {
        Connection metadata = requireSession(connId).metadata();
        if (metadata == null)
            throw new SQLException("Metadata connection is invalid for connection id: " + connId);
        return metadata;
    }

    /** Reopen only the metadata connection when FAILURE indicates it is dead. */
    public boolean reconnectMetadataIfInvalid(int connId, SQLException failure)
            throws SQLException {
        Session session = connections.get(connId);
        if (session == null) {
            return false;
        }
        Connection metadata;
        synchronized (session) {
            metadata = session.metadata();
            if (sessionUsable(metadata, failure)) {
                return false;
            }
            Connection replacement = openConnection(
                session.url, session.props, session.connectTimeoutSeconds,
                session.driverClass);
            try {
                configureMetadataConnection(replacement, session.networkTimeoutSeconds);
                session.setMetadata(replacement);
            } catch (SQLException | RuntimeException e) {
                closeQuietly(replacement);
                throw e;
            }
        }
        closeAsync(metadata);
        return true;
    }

    /** Detach the metadata connection for {@code connId}, then close it off-thread. */
    public void invalidateMetadata(int connId) {
        Session session = connections.get(connId);
        if (session == null) {
            return;
        }
        Connection metadata;
        synchronized (session) {
            metadata = session.metadata();
            session.setMetadata(null);
        }
        closeAsync(metadata);
    }

    /** Return whether {@code connId} runs schema-wide listings on a bulk session. */
    public boolean usesBulkSession(int connId) throws SQLException {
        return requireSession(connId).bulkEligible();
    }

    /**
     * Thrown for a request on the bulk lane of a connection that has no bulk
     * session: its logon was refused, so the request belongs on the metadata
     * session instead.
     */
    public static final class BulkSessionUnavailable extends SQLException {
        public BulkSessionUnavailable(int connId, Throwable cause) {
            super("Bulk session is unavailable for connection id: " + connId, cause);
        }
    }

    /**
     * Open {@code connId}'s bulk session unless it already has one.  Return
     * whether this call opened it, so the caller can restore the current
     * schema on a fresh session.  Callers are serialized by the connection's
     * bulk lock, so the logon runs outside the session monitor and cannot
     * stall a concurrent commit or rollback.
     *
     * <p>A refused logon, for example when the account has no session left,
     * ends bulk sessions for this connection: its listings return to the
     * metadata session, as on products that never use one, and the logon is
     * not attempted again.
     */
    public boolean openBulkIfAbsent(int connId) throws SQLException {
        Session session = requireSession(connId);
        if (!session.bulkEligible()) {
            throw new BulkSessionUnavailable(connId, null);
        }
        if (session.bulk() != null) {
            return false;
        }
        Connection bulk;
        try {
            bulk = openConnection(
                session.url, session.props, session.connectTimeoutSeconds,
                session.driverClass);
            try {
                configureMetadataConnection(bulk, session.networkTimeoutSeconds);
            } catch (SQLException | RuntimeException e) {
                closeQuietly(bulk);
                throw e;
            }
        } catch (SQLException | RuntimeException e) {
            session.refuseBulk();
            LOG.log(System.Logger.Level.WARNING,
                "Bulk session refused for connection {0}; schema-wide listings "
                    + "use the metadata session: {1}", connId, e.getMessage());
            throw new BulkSessionUnavailable(connId, e);
        }
        synchronized (session) {
            session.setBulk(bulk);
        }
        // A poison or force-disconnect during the logon has already closed
        // this session's connections; do not leave the new one behind.
        if (connections.get(connId) != session) {
            closeQuietly(bulk);
            throw new SQLException("Unknown connection id: " + connId);
        }
        return true;
    }

    /** Return the open bulk session for {@code connId}, or throw if it has none. */
    public Connection getBulk(int connId) throws SQLException {
        Connection bulk = requireSession(connId).bulk();
        if (bulk == null)
            throw new SQLException("Bulk connection is not open for connection id: " + connId);
        return bulk;
    }

    /** Detach the bulk session for {@code connId}, then close it off-thread. */
    public void invalidateBulk(int connId) {
        Session session = connections.get(connId);
        if (session == null) {
            return;
        }
        Connection bulk;
        synchronized (session) {
            bulk = session.bulk();
            session.setBulk(null);
        }
        closeAsync(bulk);
    }

    /**
     * Drop {@code connId}'s bulk session when {@code failure} shows it is dead,
     * so the next listing opens a fresh one; return whether it was dropped.
     */
    public boolean invalidateBulkIfInvalid(int connId, SQLException failure) {
        Session session = connections.get(connId);
        if (session == null) {
            return false;
        }
        // Validate outside the monitor: isValid is a round trip, and a
        // concurrent commit or rollback must not wait for it.
        Connection bulk = session.bulk();
        if (bulk == null || sessionUsable(bulk, failure)) {
            return false;
        }
        synchronized (session) {
            if (session.bulk() != bulk) {
                return false;
            }
            session.setBulk(null);
        }
        closeAsync(bulk);
        return true;
    }

    private void closeAsync(Connection connection) {
        if (connection == null) {
            return;
        }
        try {
            networkTimeoutExecutor.execute(() -> closeQuietly(connection));
        } catch (RejectedExecutionException e) {
            closeQuietly(connection);
        }
    }

    /** Remember the logical session schema so metadata recovery can restore it. */
    public void rememberCurrentSchema(int connId, String schema) throws SQLException {
        requireSession(connId).currentSchema = schema;
    }

    /** Return the logical session schema last set by the client, or null. */
    public String currentSchema(int connId) throws SQLException {
        return requireSession(connId).currentSchema;
    }

    private boolean sessionUsable(Connection connection, SQLException failure) {
        if (isConnectionFailure(failure)) {
            return false;
        }
        if (connection == null) {
            return false;
        }
        try {
            if (connection.isClosed()) {
                return false;
            }
        } catch (SQLException e) {
            return false;
        }
        try {
            return connection.isValid(1);
        } catch (SQLFeatureNotSupportedException | AbstractMethodError e) {
            logUnsupportedCapability("isValid(1s)", e);
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    /** Return whether FAILURE means its JDBC connection must not be reused. */
    public static boolean isConnectionFailure(Throwable failure) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        return isConnectionFailure(failure, seen);
    }

    private static boolean isConnectionFailure(Throwable failure, Set<Throwable> seen) {
        if (failure == null || !seen.add(failure)) {
            return false;
        }
        if (failure instanceof SQLRecoverableException) {
            return true;
        }
        if (failure instanceof SQLException sqlException
            && (sqlException.getErrorCode() == 12592
                || sqlException.getSQLState() != null
                    && sqlException.getSQLState().startsWith("08"))) {
            return true;
        }
        if (isConnectionFailure(failure.getCause(), seen)) {
            return true;
        }
        return failure instanceof SQLException sqlException
            && isConnectionFailure(sqlException.getNextException(), seen);
    }

    /** Close and remove the connection for {@code connId}. No-op if already closed. */
    public void disconnect(int connId) throws SQLException {
        Session session = connections.remove(connId);
        if (session != null) {
            closeSession(session);
        }
    }

    /**
     * Remove an unsafe logical connection immediately, then close its JDBC
     * sessions off-thread so a non-cooperative driver cannot delay invalidation.
     */
    public void poison(int connId) {
        Session session = connections.remove(connId);
        if (session == null) {
            return;
        }
        try {
            networkTimeoutExecutor.execute(() -> closeSession(session));
        } catch (RejectedExecutionException e) {
            closeSession(session);
        }
    }

    /** Close all connections and shut down the network-timeout executor. */
    public void disconnectAll() {
        for (Map.Entry<Integer, Session> e : connections.entrySet()) {
            closeSession(e.getValue());
        }
        connections.clear();
        networkTimeoutExecutor.shutdownNow();
    }

    private void closeSession(Session session) {
        closeQuietly(session.bulk());
        closeQuietly(session.metadata());
        closeQuietly(session.primary());
    }

    private void closeQuietly(Connection connection) {
        if (connection == null) {
            return;
        }
        try {
            if (!connection.isClosed()) {
                connection.close();
            }
        } catch (Exception e) {
            LOG.log(System.Logger.Level.WARNING,
                "Failed to close JDBC connection during cleanup", e);
        }
    }

    private static final class Session {
        private final Connection primary;
        private volatile Connection metadata;
        /** Third session for schema-wide listings; opened on first use, Oracle only. */
        private volatile Connection bulk;
        private volatile boolean bulkEligible;
        private final String url;
        private final Properties props;
        private final Integer connectTimeoutSeconds;
        private final Integer networkTimeoutSeconds;
        private final String driverClass;
        private final long validateAfterIdleMillis;
        private volatile long lastPrimaryUseMillis;
        private volatile long lastMetadataUseMillis;
        private volatile long lastBulkUseMillis;
        private volatile String currentSchema;
        private final Map<Integer, Savepoint> savepoints = new HashMap<>();

        private Session(Connection primary, Connection metadata, String url,
                        Properties props, Integer connectTimeoutSeconds,
                        Integer networkTimeoutSeconds, String driverClass,
                        long validateAfterIdleMillis, long connectedAtMillis,
                        boolean bulkEligible) {
            this.primary = primary;
            this.metadata = metadata;
            this.url = url;
            this.props = new Properties();
            this.props.putAll(props);
            this.connectTimeoutSeconds = connectTimeoutSeconds;
            this.networkTimeoutSeconds = networkTimeoutSeconds;
            this.driverClass = driverClass;
            this.validateAfterIdleMillis = validateAfterIdleMillis;
            this.lastPrimaryUseMillis = connectedAtMillis;
            this.lastMetadataUseMillis = connectedAtMillis;
            this.bulkEligible = bulkEligible;
        }

        private Connection primary() {
            return primary;
        }

        private Connection metadata() {
            return metadata;
        }

        private void setMetadata(Connection metadata) {
            this.metadata = metadata;
        }

        private Connection bulk() {
            return bulk;
        }

        private void setBulk(Connection bulk) {
            this.bulk = bulk;
        }

        private boolean bulkEligible() {
            return bulkEligible;
        }

        private void refuseBulk() {
            this.bulkEligible = false;
        }

        private void markPrimaryUsed(long nowMillis) {
            this.lastPrimaryUseMillis = nowMillis;
        }

        private long lastUseMillis(CursorManager.Lane lane) {
            return lane == CursorManager.Lane.BULK ? lastBulkUseMillis : lastMetadataUseMillis;
        }

        private void markUsed(CursorManager.Lane lane, long nowMillis) {
            if (lane == CursorManager.Lane.BULK) {
                lastBulkUseMillis = nowMillis;
            } else {
                lastMetadataUseMillis = nowMillis;
            }
        }

        private Savepoint savepoint(int savepointId) throws SQLException {
            Savepoint savepoint = savepoints.get(savepointId);
            if (savepoint == null) {
                throw new SQLException("Unknown savepoint id: " + savepointId);
            }
            return savepoint;
        }
    }
}
