package clutch.jdbc.handler;

import clutch.jdbc.ConnectionManager;
import clutch.jdbc.CursorManager;
import clutch.jdbc.model.Request;
import clutch.jdbc.model.Response;

import java.sql.*;
import java.util.*;

/**
 * Routes incoming requests to the appropriate handler method.
 * All methods return a Response; exceptions are caught at the Agent level.
 */
public class Dispatcher {

    private final ConnectionManager connMgr;
    private final CursorManager cursorMgr;

    /** Create a Dispatcher backed by the given connection and cursor managers. */
    public Dispatcher(ConnectionManager connMgr, CursorManager cursorMgr) {
        this.connMgr = connMgr;
        this.cursorMgr = cursorMgr;
    }

    /** Route {@code req} to the appropriate handler and return its response. */
    public Response dispatch(Request req) throws Exception {
        return switch (req.op) {
            case "ping"            -> ping(req);
            case "connect"         -> connect(req);
            case "disconnect"      -> disconnect(req);
            case "execute"         -> execute(req);
            case "fetch"           -> fetch(req);
            case "close-cursor"    -> closeCursor(req);
            case "get-schemas"     -> getSchemas(req);
            case "get-tables"      -> getTables(req);
            case "get-columns"     -> getColumns(req);
            case "get-primary-keys"-> getPrimaryKeys(req);
            case "get-foreign-keys"-> getForeignKeys(req);
            default -> Response.error(req.id, "Unknown op: " + req.op);
        };
    }

    // -------------------------------------------------------------------------
    // Basic
    // -------------------------------------------------------------------------

    private Response ping(Request req) {
        return Response.ok(req.id, Map.of("pong", true));
    }

    // -------------------------------------------------------------------------
    // Connection
    // -------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private Response connect(Request req) throws SQLException {
        String url      = (String) req.params.get("url");
        String user     = (String) req.params.get("user");
        String password = (String) req.params.get("password");
        Map<String, String> props =
            (Map<String, String>) req.params.getOrDefault("props", Map.of());
        Integer networkTimeoutSeconds = getOptionalInt(req, "network-timeout-seconds");

        if (url == null)
            return Response.error(req.id, "connect: 'url' is required");

        int connId = connMgr.connect(url, user, password, props, networkTimeoutSeconds);
        return Response.ok(req.id, Map.of("conn-id", connId));
    }

    private Response disconnect(Request req) throws SQLException {
        int connId = getInt(req, "conn-id");
        cursorMgr.closeForConnection(connId);
        connMgr.disconnect(connId);
        return Response.ok(req.id, Map.of("conn-id", connId));
    }

    // -------------------------------------------------------------------------
    // Execute / Fetch / Close
    // -------------------------------------------------------------------------

    private Response execute(Request req) throws Exception {
        int connId    = getInt(req, "conn-id");
        String sql    = getString(req, "sql").stripTrailing().replaceAll(";+$", "");
        int fetchSize = (int) req.params.getOrDefault("fetch-size", 200);

        Connection conn = connMgr.get(connId);
        Statement stmt  = conn.createStatement();

        boolean isQuery = stmt.execute(sql);

        if (!isQuery) {
            // DML: return affected-rows, no cursor.
            int affected = stmt.getUpdateCount();
            stmt.close();
            return Response.ok(req.id, Map.of(
                "type",          "dml",
                "affected-rows", affected
            ));
        }

        // SELECT: open cursor, return first batch.
        // setFetchSize is applied to the ResultSet after execute() succeeds,
        // not before — setting it on Statement before execute() can cause
        // Oracle 11g JDBC to hang on parse errors instead of throwing SQLException.
        ResultSet rs   = stmt.getResultSet();
        rs.setFetchSize(fetchSize);
        int cursorId   = cursorMgr.register(connId, stmt, rs);
        CursorManager.FetchResult first = cursorMgr.fetch(cursorId, fetchSize);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("type",      "query");
        result.put("cursor-id", first.done() ? null : cursorId);
        result.put("columns",   first.columns());
        result.put("col-types", first.types());
        result.put("rows",      first.rows());
        result.put("done",      first.done());
        return Response.ok(req.id, result);
    }

    private Response fetch(Request req) throws Exception {
        int cursorId  = getInt(req, "cursor-id");
        int fetchSize = (int) req.params.getOrDefault("fetch-size", 200);

        CursorManager.FetchResult fr = cursorMgr.fetch(cursorId, fetchSize);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("cursor-id", fr.done() ? null : cursorId);
        result.put("rows",      fr.rows());
        result.put("done",      fr.done());
        return Response.ok(req.id, result);
    }

    private Response closeCursor(Request req) {
        int cursorId = getInt(req, "cursor-id");
        cursorMgr.close(cursorId);
        return Response.ok(req.id, Map.of("cursor-id", cursorId));
    }

    // -------------------------------------------------------------------------
    // Metadata
    // -------------------------------------------------------------------------

    private Response getSchemas(Request req) throws SQLException {
        int connId = getInt(req, "conn-id");
        DatabaseMetaData meta = connMgr.get(connId).getMetaData();
        List<String> schemas = new ArrayList<>();
        try (ResultSet rs = meta.getSchemas()) {
            while (rs.next()) schemas.add(rs.getString("TABLE_SCHEM"));
        }
        return Response.ok(req.id, Map.of("schemas", schemas));
    }

    private Response getTables(Request req) throws SQLException {
        int connId      = getInt(req, "conn-id");
        String schema   = (String) req.params.get("schema");
        Connection conn = connMgr.get(connId);
        List<Map<String, Object>> tables = isOracle(conn)
            ? getOracleTables(conn, schema)
            : getJdbcMetadataTables(conn, schema);
        return Response.ok(req.id, Map.of("tables", tables));
    }

    private List<Map<String, Object>> getJdbcMetadataTables(Connection conn, String schema)
            throws SQLException {
        String[] types = { "TABLE", "VIEW" };
        DatabaseMetaData meta = conn.getMetaData();
        List<Map<String, Object>> tables = new ArrayList<>();
        try (ResultSet rs = meta.getTables(null, schema, "%", types)) {
            while (rs.next()) {
                tables.add(Map.of(
                    "name",   rs.getString("TABLE_NAME"),
                    "type",   rs.getString("TABLE_TYPE"),
                    "schema", Objects.toString(rs.getString("TABLE_SCHEM"), "")
                ));
            }
        }
        return tables;
    }

    private List<Map<String, Object>> getOracleTables(Connection conn, String schema)
            throws SQLException {
        String sql;
        String currentUser = currentOracleUser(conn);
        boolean hasSchema = schema != null && !schema.isBlank();
        boolean currentUserSchema = hasSchema && schema.equalsIgnoreCase(currentUser);
        if (!hasSchema || currentUserSchema) {
            sql = """
                SELECT table_name AS object_name, 'TABLE' AS object_type,
                       ? AS owner
                FROM user_tables
                UNION ALL
                SELECT view_name AS object_name, 'VIEW' AS object_type,
                       ? AS owner
                FROM user_views
                ORDER BY object_name
                """;
        } else {
            sql = """
                SELECT table_name AS object_name, 'TABLE' AS object_type, owner
                FROM all_tables
                WHERE owner = ?
                UNION ALL
                SELECT view_name AS object_name, 'VIEW' AS object_type, owner
                FROM all_views
                WHERE owner = ?
                ORDER BY object_name
                """;
        }
        List<Map<String, Object>> tables = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setQueryTimeout(15);
            if (!hasSchema || currentUserSchema) {
                String owner = currentUser != null ? currentUser : Objects.toString(schema, "");
                ps.setString(1, owner);
                ps.setString(2, owner);
            } else {
                String owner = schema.toUpperCase(Locale.ROOT);
                ps.setString(1, owner);
                ps.setString(2, owner);
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tables.add(Map.of(
                        "name", rs.getString("object_name"),
                        "type", rs.getString("object_type"),
                        "schema", Objects.toString(rs.getString("owner"), "")
                    ));
                }
            }
        }
        return tables;
    }

    private Response getColumns(Request req) throws SQLException {
        int connId    = getInt(req, "conn-id");
        String schema = (String) req.params.get("schema");
        String table  = getString(req, "table");

        DatabaseMetaData meta = connMgr.get(connId).getMetaData();
        List<Map<String, Object>> cols = new ArrayList<>();
        try (ResultSet rs = meta.getColumns(null, schema, table, "%")) {
            while (rs.next()) {
                cols.add(Map.of(
                    "name",     rs.getString("COLUMN_NAME"),
                    "type",     rs.getString("TYPE_NAME"),
                    "nullable", rs.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls,
                    "position", rs.getInt("ORDINAL_POSITION")
                ));
            }
        }
        return Response.ok(req.id, Map.of("columns", cols));
    }

    private Response getPrimaryKeys(Request req) throws SQLException {
        int connId    = getInt(req, "conn-id");
        String schema = (String) req.params.get("schema");
        String table  = getString(req, "table");

        DatabaseMetaData meta = connMgr.get(connId).getMetaData();
        List<String> pks = new ArrayList<>();
        try (ResultSet rs = meta.getPrimaryKeys(null, schema, table)) {
            while (rs.next()) pks.add(rs.getString("COLUMN_NAME"));
        }
        return Response.ok(req.id, Map.of("primary-keys", pks));
    }

    private Response getForeignKeys(Request req) throws SQLException {
        int connId    = getInt(req, "conn-id");
        String schema = (String) req.params.get("schema");
        String table  = getString(req, "table");

        DatabaseMetaData meta = connMgr.get(connId).getMetaData();
        List<Map<String, Object>> fks = new ArrayList<>();
        try (ResultSet rs = meta.getImportedKeys(null, schema, table)) {
            while (rs.next()) {
                fks.add(Map.of(
                    "fk-column",   rs.getString("FKCOLUMN_NAME"),
                    "pk-table",    rs.getString("PKTABLE_NAME"),
                    "pk-schema",   Objects.toString(rs.getString("PKTABLE_SCHEM"), ""),
                    "pk-column",   rs.getString("PKCOLUMN_NAME")
                ));
            }
        }
        return Response.ok(req.id, Map.of("foreign-keys", fks));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private int getInt(Request req, String key) {
        Object v = req.params.get(key);
        if (v instanceof Number n) return n.intValue();
        throw new IllegalArgumentException("Missing or non-integer param: " + key);
    }

    private String getString(Request req, String key) {
        Object v = req.params.get(key);
        if (v instanceof String s) return s;
        throw new IllegalArgumentException("Missing or non-string param: " + key);
    }

    private Integer getOptionalInt(Request req, String key) {
        Object v = req.params.get(key);
        if (v == null) return null;
        if (v instanceof Number n) return n.intValue();
        throw new IllegalArgumentException("Non-integer param: " + key);
    }

    private boolean isOracle(Connection conn) throws SQLException {
        String productName = conn.getMetaData().getDatabaseProductName();
        return productName != null && productName.toLowerCase(Locale.ROOT).contains("oracle");
    }

    private String currentOracleUser(Connection conn) throws SQLException {
        String user = conn.getMetaData().getUserName();
        if (user == null) return null;
        int at = user.indexOf('@');
        if (at >= 0) user = user.substring(0, at);
        int bracket = user.indexOf('[');
        if (bracket >= 0) user = user.substring(0, bracket);
        return user.strip().toUpperCase(Locale.ROOT);
    }
}
