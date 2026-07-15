package clutch.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLRecoverableException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.IdentityHashMap;
import java.util.Map;
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

    private final AtomicInteger nextId = new AtomicInteger(1);
    private final Map<Integer, Session> connections = new ConcurrentHashMap<>();
    private final ExecutorService networkTimeoutExecutor = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "clutch-jdbc-network-timeout");
        t.setDaemon(true);
        return t;
    });

    /**
     * Open a new JDBC connection and return its assigned connId.
     *
     * @param url      full JDBC URL (e.g. "jdbc:oracle:thin:@//host:1521/ORCL")
     * @param user     database user (may be null for URL-embedded credentials)
     * @param password database password (may be null)
     * @param props    extra driver properties (e.g. oracle.net.tns_admin)
     */
    public int connect(String url, String user, String password, Map<String, String> props,
                       Integer connectTimeoutSeconds, Integer networkTimeoutSeconds,
                       boolean autoCommit, String driverClass)
            throws SQLException {
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
                    networkTimeoutSeconds, driverClass));
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

    private void configurePrimaryConnection(Connection conn, boolean autoCommit,
                                            Integer networkTimeoutSeconds)
            throws SQLException {
        if (!autoCommit) {
            try {
                conn.setAutoCommit(false);
            } catch (SQLFeatureNotSupportedException | AbstractMethodError e) {
                logUnsupportedCapability("setAutoCommit(false)", e);
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
        Session session = connections.get(connId);
        if (session == null)
            throw new SQLException("Unknown connection id: " + connId);
        return session.primary();
    }

    /** Return the live metadata Connection for {@code connId}, or throw if unknown. */
    public Connection getMetadata(int connId) throws SQLException {
        Session session = connections.get(connId);
        if (session == null)
            throw new SQLException("Unknown connection id: " + connId);
        Connection metadata = session.metadata();
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
        synchronized (session) {
            Connection metadata = session.metadata();
            if (metadataUsable(metadata, failure)) {
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
            closeQuietly(metadata);
            return true;
        }
    }

    /** Reopen only the metadata connection when its JDBC liveness check fails. */
    public boolean reconnectMetadataIfInvalid(int connId) throws SQLException {
        return reconnectMetadataIfInvalid(connId, null);
    }

    /** Close and invalidate only the metadata connection for {@code connId}. */
    public void invalidateMetadata(int connId) {
        Session session = connections.get(connId);
        if (session == null) {
            return;
        }
        synchronized (session) {
            Connection metadata = session.metadata();
            session.setMetadata(null);
            closeQuietly(metadata);
        }
    }

    /** Remember the logical session schema so metadata recovery can restore it. */
    public void rememberCurrentSchema(int connId, String schema) throws SQLException {
        Session session = connections.get(connId);
        if (session == null) {
            throw new SQLException("Unknown connection id: " + connId);
        }
        session.currentSchema = schema;
    }

    /** Return the logical session schema last set by the client, or null. */
    public String currentSchema(int connId) throws SQLException {
        Session session = connections.get(connId);
        if (session == null) {
            throw new SQLException("Unknown connection id: " + connId);
        }
        return session.currentSchema;
    }

    private boolean metadataUsable(Connection connection, SQLException failure) {
        if (isConnectionFailure(failure)) {
            return false;
        }
        try {
            return connection != null && !connection.isClosed() && connection.isValid(1);
        } catch (SQLException | AbstractMethodError e) {
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
        private final String url;
        private final Properties props;
        private final Integer connectTimeoutSeconds;
        private final Integer networkTimeoutSeconds;
        private final String driverClass;
        private volatile String currentSchema;

        private Session(Connection primary, Connection metadata, String url,
                        Properties props, Integer connectTimeoutSeconds,
                        Integer networkTimeoutSeconds, String driverClass) {
            this.primary = primary;
            this.metadata = metadata;
            this.url = url;
            this.props = new Properties();
            this.props.putAll(props);
            this.connectTimeoutSeconds = connectTimeoutSeconds;
            this.networkTimeoutSeconds = networkTimeoutSeconds;
            this.driverClass = driverClass;
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
    }
}
