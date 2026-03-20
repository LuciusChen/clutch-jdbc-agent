package clutch.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Maintains a map of connId → logical JDBC session.
 * Each session owns a primary connection for foreground queries and a separate
 * metadata connection for schema/object introspection.
 */
public class ConnectionManager {

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
                       boolean autoCommit)
            throws SQLException {
        Properties p = new Properties();
        if (props != null) p.putAll(props);
        if (user != null)     p.setProperty("user",     user);
        if (password != null) p.setProperty("password", password);

        Connection primary = openConnection(url, p, connectTimeoutSeconds);
        try {
            configurePrimaryConnection(primary, autoCommit, networkTimeoutSeconds);
            Connection metadata = openConnection(url, p, connectTimeoutSeconds);
            try {
                configureMetadataConnection(metadata, networkTimeoutSeconds);
                int id = nextId.getAndIncrement();
                connections.put(id, new Session(primary, metadata));
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
            } catch (SQLFeatureNotSupportedException | AbstractMethodError ignored) {
                // Driver does not support manual-commit mode; proceed with autocommit.
            }
        }
        applyNetworkTimeout(conn, networkTimeoutSeconds);
    }

    private void configureMetadataConnection(Connection conn, Integer networkTimeoutSeconds)
            throws SQLException {
        try {
            conn.setAutoCommit(true);
        } catch (SQLFeatureNotSupportedException | AbstractMethodError ignored) {
            // Driver does not allow autocommit changes; continue.
        }
        try {
            conn.setReadOnly(true);
        } catch (SQLFeatureNotSupportedException | AbstractMethodError ignored) {
            // Driver does not implement read-only mode; continue.
        }
        applyNetworkTimeout(conn, networkTimeoutSeconds);
    }

    private void applyNetworkTimeout(Connection conn, Integer networkTimeoutSeconds)
            throws SQLException {
        if (networkTimeoutSeconds != null && networkTimeoutSeconds > 0) {
            try {
                conn.setNetworkTimeout(networkTimeoutExecutor, networkTimeoutSeconds * 1000);
            } catch (SQLFeatureNotSupportedException | AbstractMethodError ignored) {
                // Driver does not implement network timeout; continue without it.
            }
        }
    }

    private Connection openConnection(String url, Properties props, Integer connectTimeoutSeconds)
            throws SQLException {
        if (connectTimeoutSeconds == null || connectTimeoutSeconds <= 0) {
            return DriverManager.getConnection(url, props);
        }
        synchronized (DriverManager.class) {
            int previousTimeout = DriverManager.getLoginTimeout();
            DriverManager.setLoginTimeout(connectTimeoutSeconds);
            try {
                return DriverManager.getConnection(url, props);
            } finally {
                DriverManager.setLoginTimeout(previousTimeout);
            }
        }
    }

    /** Return the live primary Connection for {@code connId}, or throw if unknown. */
    public Connection get(int connId) throws SQLException {
        return getPrimary(connId);
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
        return session.metadata();
    }

    /** Close and remove the connection for {@code connId}. No-op if already closed. */
    public void disconnect(int connId) throws SQLException {
        Session session = connections.remove(connId);
        if (session != null) {
            closeQuietly(session.metadata());
            closeQuietly(session.primary());
        }
    }

    /** Close all connections and shut down the network-timeout executor. */
    public void disconnectAll() {
        for (Map.Entry<Integer, Session> e : connections.entrySet()) {
            closeQuietly(e.getValue().metadata());
            closeQuietly(e.getValue().primary());
        }
        connections.clear();
        networkTimeoutExecutor.shutdownNow();
    }

    private void closeQuietly(Connection connection) {
        if (connection == null) {
            return;
        }
        try {
            if (!connection.isClosed()) {
                connection.close();
            }
        } catch (Exception ignored) {}
    }

    private record Session(Connection primary, Connection metadata) {}
}
