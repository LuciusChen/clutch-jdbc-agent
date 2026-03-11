package clutch.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Maintains a map of connId → Connection.
 * No pooling — one JDBC Connection per clutch connection.
 */
public class ConnectionManager {

    private final AtomicInteger nextId = new AtomicInteger(1);
    private final Map<Integer, Connection> connections = new ConcurrentHashMap<>();

    /**
     * Open a new JDBC connection and return its assigned connId.
     *
     * @param url      full JDBC URL (e.g. "jdbc:oracle:thin:@//host:1521/ORCL")
     * @param user     database user (may be null for URL-embedded credentials)
     * @param password database password (may be null)
     * @param props    extra driver properties (e.g. oracle.net.tns_admin)
     */
    public int connect(String url, String user, String password, Map<String, String> props)
            throws SQLException {
        Properties p = new Properties();
        if (props != null) p.putAll(props);
        if (user != null)     p.setProperty("user",     user);
        if (password != null) p.setProperty("password", password);

        Connection conn = DriverManager.getConnection(url, p);
        int id = nextId.getAndIncrement();
        connections.put(id, conn);
        return id;
    }

    public Connection get(int connId) throws SQLException {
        Connection c = connections.get(connId);
        if (c == null)
            throw new SQLException("Unknown connection id: " + connId);
        return c;
    }

    public void disconnect(int connId) throws SQLException {
        Connection c = connections.remove(connId);
        if (c != null && !c.isClosed()) c.close();
    }

    public void disconnectAll() {
        for (Map.Entry<Integer, Connection> e : connections.entrySet()) {
            try { e.getValue().close(); } catch (Exception ignored) {}
        }
        connections.clear();
    }
}
