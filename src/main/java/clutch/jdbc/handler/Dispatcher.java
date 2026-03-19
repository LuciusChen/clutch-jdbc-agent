package clutch.jdbc.handler;

import clutch.jdbc.ConnectionManager;
import clutch.jdbc.CursorManager;
import clutch.jdbc.model.Request;
import clutch.jdbc.model.Response;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Routes incoming requests to the appropriate handler method.
 * All methods return a Response; exceptions are caught at the Agent level.
 */
public class Dispatcher {

    private static final int DEFAULT_FETCH_SIZE              = 500;
    private static final int ORACLE_TABLES_TIMEOUT_SECONDS   =  15;
    private static final int ORACLE_METADATA_TIMEOUT_SECONDS =   5;
    static final int DEFAULT_EXECUTE_TIMEOUT                 =  29; // s; safety net when no client timeout given
    private static final List<String> ORACLE_SYSTEM_OWNERS = List.of(
        "SYS", "SYSTEM", "XDB", "MDSYS", "CTXSYS", "LBACSYS", "OLAPSYS",
        "WMSYS", "DBSNMP", "APPQOSSYS", "AUDSYS", "DVSYS",
        "GSMADMIN_INTERNAL", "OJVMSYS", "OUTLN"
    );
    private static final Set<String> ORACLE_SYSTEM_OWNER_SET = Set.copyOf(ORACLE_SYSTEM_OWNERS);
    private static final String ORACLE_SYSTEM_OWNERS_SQL =
        ORACLE_SYSTEM_OWNERS.stream()
            .map(owner -> "'" + owner + "'")
            .collect(Collectors.joining(", "));

    private final ConnectionManager connMgr;
    private final CursorManager cursorMgr;
    private final ExecutorService executePool = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "clutch-jdbc-execute");
        t.setDaemon(true);
        return t;
    });

    /** Create a Dispatcher backed by the given connection and cursor managers. */
    public Dispatcher(ConnectionManager connMgr, CursorManager cursorMgr) {
        this.connMgr = connMgr;
        this.cursorMgr = cursorMgr;
    }

    /** Shut down the execute thread pool. Called on agent shutdown. */
    public void shutdown() {
        executePool.shutdownNow();
    }

    /** Route {@code req} to the appropriate handler and return its response. */
    public Response dispatch(Request req) throws Exception {
        return switch (req.op) {
            case "ping"            -> ping(req);
            case "connect"         -> connect(req);
            case "disconnect"      -> disconnect(req);
            case "commit"          -> commit(req);
            case "rollback"        -> rollback(req);
            case "set-auto-commit" -> setAutoCommit(req);
            case "execute"         -> execute(req);
            case "fetch"           -> fetch(req);
            case "close-cursor"    -> closeCursor(req);
            case "get-schemas"     -> getSchemas(req);
            case "get-tables"      -> getTables(req);
            case "search-tables"   -> searchTables(req);
            case "get-columns"     -> getColumns(req);
            case "search-columns"  -> searchColumns(req);
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
        Integer connectTimeoutSeconds = getOptionalInt(req, "connect-timeout-seconds");
        Integer networkTimeoutSeconds = getOptionalInt(req, "network-timeout-seconds");
        Object autoCommitValue = req.params.get("auto-commit");
        boolean autoCommit = autoCommitValue == null || Boolean.TRUE.equals(autoCommitValue);

        if (url == null)
            return Response.error(req.id, "connect: 'url' is required");

        int connId = connMgr.connect(url, user, password, props,
            connectTimeoutSeconds, networkTimeoutSeconds, autoCommit);
        return Response.ok(req.id, Map.of("conn-id", connId));
    }

    private Response disconnect(Request req) throws SQLException {
        int connId = getInt(req, "conn-id");
        cursorMgr.closeForConnection(connId);
        connMgr.disconnect(connId);
        return Response.ok(req.id, Map.of("conn-id", connId));
    }

    private Response commit(Request req) throws SQLException {
        int connId = getInt(req, "conn-id");
        connMgr.get(connId).commit();
        return Response.ok(req.id, Map.of("conn-id", connId));
    }

    private Response rollback(Request req) throws SQLException {
        int connId = getInt(req, "conn-id");
        connMgr.get(connId).rollback();
        return Response.ok(req.id, Map.of("conn-id", connId));
    }

    private Response setAutoCommit(Request req) throws SQLException {
        int connId = getInt(req, "conn-id");
        Object autoCommitValue = req.params.get("auto-commit");
        boolean autoCommit = autoCommitValue == null || Boolean.TRUE.equals(autoCommitValue);
        connMgr.get(connId).setAutoCommit(autoCommit);
        return Response.ok(req.id, Map.of("conn-id", connId, "auto-commit", autoCommit));
    }

    // -------------------------------------------------------------------------
    // Execute / Fetch / Close
    // -------------------------------------------------------------------------

    private Response execute(Request req) throws Exception {
        int connId                  = getInt(req, "conn-id");
        String sql                  = getString(req, "sql").stripTrailing().replaceAll(";+$", "");
        int fetchSize               = (int) req.params.getOrDefault("fetch-size", DEFAULT_FETCH_SIZE);
        Integer queryTimeoutSeconds = getOptionalInt(req, "query-timeout-seconds");
        int executeTimeout          = (queryTimeoutSeconds != null && queryTimeoutSeconds > 0)
                                      ? queryTimeoutSeconds : DEFAULT_EXECUTE_TIMEOUT;

        Connection conn = connMgr.get(connId);
        if (!conn.isValid(3))
            return Response.error(req.id,
                "Connection lost: the server closed the connection (idle timeout). Please reconnect.");
        Statement  stmt = conn.createStatement();
        stmt.setQueryTimeout(executeTimeout);   // Oracle-side cancel (belt)

        Future<Boolean> future = executePool.submit(() -> stmt.execute(sql));
        boolean isQuery;
        try {
            isQuery = future.get(executeTimeout + 1, TimeUnit.SECONDS); // thread-level cancel (suspenders)
        } catch (TimeoutException e) {
            future.cancel(true);
            try { stmt.cancel(); } catch (Exception ignored) {}
            try { stmt.close(); } catch (Exception ignored) {}
            return Response.error(req.id, "Query timed out after " + executeTimeout + "s");
        } catch (ExecutionException e) {
            try { stmt.close(); } catch (Exception ignored) {}
            Throwable cause = e.getCause();
            throw (cause instanceof Exception ex) ? ex : new RuntimeException(cause);
        }

        if (!isQuery) {
            // DML: return affected-rows, no cursor.
            int affected = stmt.getUpdateCount();
            stmt.close();
            return Response.ok(req.id, Map.of("type", "dml", "affected-rows", affected));
        }

        // SELECT: open cursor, return first batch.
        // setFetchSize is applied to the ResultSet after execute() succeeds,
        // not before — setting it on Statement before execute() can cause
        // Oracle 11g JDBC to hang on parse errors instead of throwing SQLException.
        try {
            ResultSet rs = stmt.getResultSet();
            rs.setFetchSize(fetchSize);
            int cursorId = cursorMgr.register(connId, stmt, rs);
            CursorManager.FetchResult first = cursorMgr.fetch(cursorId, fetchSize);
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("type",      "query");
            result.put("cursor-id", first.done() ? null : cursorId);
            result.put("columns",   first.columns());
            result.put("col-types", first.types());
            result.put("rows",      first.rows());
            result.put("done",      first.done());
            return Response.ok(req.id, result);
        } catch (Exception e) {
            try { stmt.close(); } catch (Exception ignored) {}
            throw e;
        }
    }

    private Response fetch(Request req) throws Exception {
        int cursorId  = getInt(req, "cursor-id");
        int fetchSize = (int) req.params.getOrDefault("fetch-size", DEFAULT_FETCH_SIZE);

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
        int connId    = getInt(req, "conn-id");
        String schema = (String) req.params.get("schema");
        Connection conn = connMgr.get(connId);
        return isOracle(conn)
            ? oracleTablesCursor(req.id, connId, conn, schema)
            : jdbcTablesOneBatch(req.id, conn, schema);
    }

    /**
     * Oracle path: open a cursor over user_tables/user_views and return the first
     * batch.  Subsequent batches are fetched via the normal "fetch" op.
     * fetchSize=1000 on the ResultSet reduces Oracle round-trips from O(N/10) to
     * O(N/1000) — critical for schemas with tens of thousands of tables.
     */
    private Response oracleTablesCursor(int reqId, int connId, Connection conn, String schema)
            throws SQLException {
        OracleSchema os = OracleSchema.of(currentOracleUser(conn), schema);
        String sql = os.useUserTables() ? """
                SELECT object_name AS name, object_type AS type, owner AS schema, source_owner AS source_schema
                FROM (
                  SELECT object_name, object_type, owner, source_owner, source_rank,
                         ROW_NUMBER() OVER (PARTITION BY object_name ORDER BY source_rank) AS rn
                  FROM (
                    SELECT table_name AS object_name, 'TABLE' AS object_type, ? AS owner, ? AS source_owner, 0 AS source_rank
                    FROM user_tables
                    UNION ALL
                    SELECT view_name AS object_name, 'VIEW' AS object_type, ? AS owner, ? AS source_owner, 0 AS source_rank
                    FROM user_views
                    UNION ALL
                    SELECT synonym_name AS object_name, 'SYNONYM' AS object_type, table_owner AS owner, ? AS source_owner, 1 AS source_rank
                    FROM user_synonyms
                    UNION ALL
                    SELECT table_name AS object_name, 'TABLE' AS object_type, owner, owner AS source_owner, 2 AS source_rank
                    FROM all_tables
                    WHERE owner NOT IN (%s)
                      AND owner <> ?
                    UNION ALL
                    SELECT view_name AS object_name, 'VIEW' AS object_type, owner, owner AS source_owner, 2 AS source_rank
                    FROM all_views
                    WHERE owner NOT IN (%s)
                      AND owner <> ?
                  )
                )
                WHERE rn = 1
                ORDER BY name
                """.formatted(ORACLE_SYSTEM_OWNERS_SQL, ORACLE_SYSTEM_OWNERS_SQL) : """
                SELECT table_name AS name, 'TABLE' AS type, owner AS schema, owner AS source_schema
                FROM all_tables
                WHERE owner = ?
                UNION ALL
                SELECT view_name AS name, 'VIEW' AS type, owner AS schema, owner AS source_schema
                FROM all_views
                WHERE owner = ?
                ORDER BY name
                """;
        PreparedStatement ps = conn.prepareStatement(sql);
        try {
            ps.setQueryTimeout(ORACLE_TABLES_TIMEOUT_SECONDS);
            if (os.useUserTables()) {
                ps.setString(1, os.owner());
                ps.setString(2, os.owner());
                ps.setString(3, os.owner());
                ps.setString(4, os.owner());
                ps.setString(5, os.owner());
                ps.setString(6, os.owner());
                ps.setString(7, os.owner());
            } else {
                ps.setString(1, os.owner());
                ps.setString(2, os.owner());
            }
            ResultSet rs = ps.executeQuery();
            rs.setFetchSize(1000);
            int cursorId = cursorMgr.register(connId, ps, rs);
            CursorManager.FetchResult first = cursorMgr.fetch(cursorId, 1000);
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("cursor-id", first.done() ? null : cursorId);
            result.put("columns",   List.of("name", "type", "schema", "source_schema"));
            result.put("rows",      first.rows());
            result.put("done",      first.done());
            return Response.ok(reqId, result);
        } catch (SQLException e) {
            try { ps.close(); } catch (Exception ignored) {}
            throw e;
        }
    }

    /**
     * Non-Oracle JDBC path: materialize via DatabaseMetaData in one batch.
     * Returns the same cursor-format response with done=true so the Emacs side
     * can use a single code path for both Oracle and non-Oracle.
     */
    private Response jdbcTablesOneBatch(int reqId, Connection conn, String schema)
            throws SQLException {
        String[] types = {"TABLE", "VIEW"};
        DatabaseMetaData meta = conn.getMetaData();
        List<List<Object>> rows = new ArrayList<>();
        try (ResultSet rs = meta.getTables(null, schema, "%", types)) {
            while (rs.next()) {
                List<Object> row = new ArrayList<>(3);
                row.add(rs.getString("TABLE_NAME"));
                row.add(rs.getString("TABLE_TYPE"));
                row.add(Objects.toString(rs.getString("TABLE_SCHEM"), ""));
                rows.add(row);
            }
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("cursor-id", null);
        result.put("columns",   List.of("name", "type", "schema"));
        result.put("rows",      rows);
        result.put("done",      true);
        return Response.ok(reqId, result);
    }

    private Response getColumns(Request req) throws SQLException {
        int connId    = getInt(req, "conn-id");
        String schema = (String) req.params.get("schema");
        String table  = getString(req, "table");

        Connection conn = connMgr.get(connId);
        List<Map<String, Object>> cols = isOracle(conn)
            ? getOracleColumns(conn, schema, table, null)
            : getJdbcMetadataColumns(conn, schema, table, null);
        return Response.ok(req.id, Map.of("columns", cols));
    }

    private Response searchTables(Request req) throws SQLException {
        int connId      = getInt(req, "conn-id");
        String schema   = (String) req.params.get("schema");
        String prefix   = Objects.toString(req.params.get("prefix"), "");
        Connection conn = connMgr.get(connId);
        List<Map<String, Object>> tables = isOracle(conn)
            ? searchOracleTables(conn, schema, prefix)
            : searchJdbcMetadataTables(conn, schema, prefix);
        return Response.ok(req.id, Map.of("tables", tables));
    }

    private List<Map<String, Object>> searchJdbcMetadataTables(Connection conn, String schema, String prefix)
            throws SQLException {
        String[] types = { "TABLE", "VIEW" };
        String pattern = (prefix == null || prefix.isBlank()) ? "%" : prefix + "%";
        DatabaseMetaData meta = conn.getMetaData();
        List<Map<String, Object>> tables = new ArrayList<>();
        try (ResultSet rs = meta.getTables(null, schema, pattern, types)) {
            while (rs.next()) {
                tables.add(Map.of(
                    "name",   rs.getString("TABLE_NAME"),
                    "type",   rs.getString("TABLE_TYPE"),
                    "schema", Objects.toString(rs.getString("TABLE_SCHEM"), ""),
                    "source-schema", Objects.toString(rs.getString("TABLE_SCHEM"), "")
                ));
            }
        }
        return tables;
    }

    private List<Map<String, Object>> searchOracleTables(Connection conn, String schema, String prefix)
            throws SQLException {
        OracleSchema os = OracleSchema.of(currentOracleUser(conn), schema);
        String pattern = ((prefix == null || prefix.isBlank()) ? "" : prefix)
            .toUpperCase(Locale.ROOT) + "%";
        String sql;
        if (os.useUserTables()) {
            sql = """
                SELECT object_name, object_type, owner, source_owner, source_rank
                FROM (
                  SELECT table_name AS object_name, 'TABLE' AS object_type, ? AS owner, ? AS source_owner, 0 AS source_rank
                  FROM user_tables
                  WHERE table_name LIKE ?
                  UNION ALL
                  SELECT view_name AS object_name, 'VIEW' AS object_type, ? AS owner, ? AS source_owner, 0 AS source_rank
                  FROM user_views
                  WHERE view_name LIKE ?
                  UNION ALL
                  SELECT synonym_name AS object_name, 'SYNONYM' AS object_type, table_owner AS owner, ? AS source_owner, 1 AS source_rank
                  FROM user_synonyms
                  WHERE synonym_name LIKE ?
                  UNION ALL
                  SELECT table_name AS object_name, 'TABLE' AS object_type, owner, owner AS source_owner, 2 AS source_rank
                  FROM all_tables
                  WHERE owner NOT IN (%s)
                    AND owner <> ?
                    AND table_name LIKE ?
                  UNION ALL
                  SELECT view_name AS object_name, 'VIEW' AS object_type, owner, owner AS source_owner, 2 AS source_rank
                  FROM all_views
                  WHERE owner NOT IN (%s)
                    AND owner <> ?
                    AND view_name LIKE ?
                  UNION ALL
                  SELECT synonym_name AS object_name, 'PUBLIC SYNONYM' AS object_type, table_owner AS owner, 'PUBLIC' AS source_owner, 3 AS source_rank
                  FROM all_synonyms
                  WHERE owner = 'PUBLIC' AND synonym_name LIKE ?
                )
                ORDER BY source_rank, object_name
                """.formatted(ORACLE_SYSTEM_OWNERS_SQL, ORACLE_SYSTEM_OWNERS_SQL);
        } else {
            sql = """
                SELECT object_name, object_type, owner, owner AS source_owner
                FROM (
                  SELECT table_name AS object_name, 'TABLE' AS object_type, owner
                  FROM all_tables
                  WHERE owner = ? AND table_name LIKE ?
                  UNION ALL
                  SELECT view_name AS object_name, 'VIEW' AS object_type, owner
                  FROM all_views
                  WHERE owner = ? AND view_name LIKE ?
                )
                ORDER BY object_name
                """;
        }
        LinkedHashMap<String, Map<String, Object>> tablesByName = new LinkedHashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setQueryTimeout(ORACLE_METADATA_TIMEOUT_SECONDS);
            if (os.useUserTables()) {
                ps.setString(1, os.owner());
                ps.setString(2, os.owner());
                ps.setString(3, pattern);
                ps.setString(4, os.owner());
                ps.setString(5, os.owner());
                ps.setString(6, pattern);
                ps.setString(7, os.owner());
                ps.setString(8, pattern);
                ps.setString(9, os.owner());
                ps.setString(10, pattern);
                ps.setString(11, os.owner());
                ps.setString(12, pattern);
                ps.setString(13, pattern);
            } else {
                ps.setString(1, os.owner());
                ps.setString(2, pattern);
                ps.setString(3, os.owner());
                ps.setString(4, pattern);
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String name = rs.getString("object_name");
                    String owner = Objects.toString(rs.getString("owner"), "");
                    String objectType = rs.getString("object_type");
                    String sourceOwner = Objects.toString(rs.getString("source_owner"), owner);
                    tablesByName.putIfAbsent(name, Map.of(
                        "name", name,
                        "type", objectType,
                        "schema", owner,
                        "source-schema", sourceOwner
                    ));
                }
            }
        }
        return new ArrayList<>(tablesByName.values());
    }

    private Response searchColumns(Request req) throws SQLException {
        int connId    = getInt(req, "conn-id");
        String schema = (String) req.params.get("schema");
        String table  = getString(req, "table");
        String prefix = Objects.toString(req.params.get("prefix"), "");
        Connection conn = connMgr.get(connId);
        List<Map<String, Object>> cols = isOracle(conn)
            ? searchOracleColumns(conn, schema, table, prefix)
            : getJdbcMetadataColumns(conn, schema, table, prefix);
        return Response.ok(req.id, Map.of("columns", cols));
    }

    private List<Map<String, Object>> searchOracleColumns(Connection conn, String schema,
                                                          String table, String prefix)
            throws SQLException {
        List<Map<String, Object>> cols = getOracleColumns(conn, schema, table, prefix);
        OracleSchema os = OracleSchema.of(currentOracleUser(conn), schema);
        String columnPattern = ((prefix == null || prefix.isBlank()) ? "" : prefix)
            .toUpperCase(Locale.ROOT) + "%";
        if (!cols.isEmpty() || !os.useUserTables()) {
            return cols;
        }

        OracleObject target = resolveOracleSynonym(conn, table);
        if (target != null) {
            cols = getOracleColumnsByOwner(conn, target.owner(), target.name(), columnPattern);
            if (!cols.isEmpty()) {
                return cols;
            }
        }

        return searchOracleAccessibleColumns(conn, table, prefix);
    }

    private List<Map<String, Object>> getJdbcMetadataColumns(Connection conn, String schema,
                                                             String table, String prefix)
            throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        String pattern = (prefix == null || prefix.isBlank()) ? "%" : prefix + "%";
        List<Map<String, Object>> cols = new ArrayList<>();
        try (ResultSet rs = meta.getColumns(null, schema, table, pattern)) {
            while (rs.next()) {
                cols.add(Map.of(
                    "name",     rs.getString("COLUMN_NAME"),
                    "type",     rs.getString("TYPE_NAME"),
                    "nullable", rs.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls,
                    "position", rs.getInt("ORDINAL_POSITION")
                ));
            }
        }
        return cols;
    }

    private List<Map<String, Object>> getOracleColumns(Connection conn, String schema,
                                                       String table, String prefix)
            throws SQLException {
        OracleSchema os = OracleSchema.of(currentOracleUser(conn), schema);
        String columnPattern = ((prefix == null || prefix.isBlank()) ? "" : prefix)
            .toUpperCase(Locale.ROOT) + "%";
        return os.useUserTables()
            ? getOracleUserColumns(conn, table, columnPattern)
            : getOracleColumnsByOwner(conn, os.owner(), table, columnPattern);
    }

    private List<Map<String, Object>> getOracleUserColumns(Connection conn, String table,
                                                           String columnPattern)
            throws SQLException {
        String sql = """
            SELECT column_name, data_type, nullable, column_id
            FROM user_tab_columns
            WHERE table_name = ?
              AND column_name LIKE ?
            ORDER BY column_id
            """;
        List<Map<String, Object>> cols = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setQueryTimeout(ORACLE_METADATA_TIMEOUT_SECONDS);
            ps.setString(1, table.toUpperCase(Locale.ROOT));
            ps.setString(2, columnPattern);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    cols.add(oracleColumnRow(rs));
                }
            }
        }
        return cols;
    }

    private List<Map<String, Object>> getOracleColumnsByOwner(Connection conn, String owner,
                                                              String table, String columnPattern)
            throws SQLException {
        String sql = """
            SELECT column_name, data_type, nullable, column_id
            FROM all_tab_columns
            WHERE owner = ?
              AND table_name = ?
              AND column_name LIKE ?
            ORDER BY column_id
            """;
        List<Map<String, Object>> cols = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setQueryTimeout(ORACLE_METADATA_TIMEOUT_SECONDS);
            ps.setString(1, owner.toUpperCase(Locale.ROOT));
            ps.setString(2, table.toUpperCase(Locale.ROOT));
            ps.setString(3, columnPattern);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    cols.add(oracleColumnRow(rs));
                }
            }
        }
        return cols;
    }

    private List<Map<String, Object>> searchOracleAccessibleColumns(Connection conn, String table,
                                                                    String prefix)
            throws SQLException {
        String columnPattern = ((prefix == null || prefix.isBlank()) ? "" : prefix)
            .toUpperCase(Locale.ROOT) + "%";
        String sql = """
            SELECT column_name, data_type, nullable, column_id
            FROM all_tab_columns
            WHERE table_name = ?
              AND column_name LIKE ?
            ORDER BY owner, column_id
            """;
        LinkedHashMap<String, Map<String, Object>> colsByName = new LinkedHashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setQueryTimeout(ORACLE_METADATA_TIMEOUT_SECONDS);
            ps.setString(1, table.toUpperCase(Locale.ROOT));
            ps.setString(2, columnPattern);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String name = rs.getString("column_name");
                    colsByName.putIfAbsent(name, oracleColumnRow(rs));
                }
            }
        }
        return new ArrayList<>(colsByName.values());
    }

    private OracleObject resolveOracleSynonym(Connection conn, String name) throws SQLException {
        String sql = """
            SELECT table_owner, table_name
            FROM user_synonyms
            WHERE synonym_name = ?
            UNION ALL
            SELECT table_owner, table_name
            FROM all_synonyms
            WHERE owner = 'PUBLIC'
              AND synonym_name = ?
            """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setQueryTimeout(ORACLE_METADATA_TIMEOUT_SECONDS);
            ps.setString(1, name.toUpperCase(Locale.ROOT));
            ps.setString(2, name.toUpperCase(Locale.ROOT));
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new OracleObject(rs.getString("table_owner"),
                                            rs.getString("table_name"));
                }
            }
        }
        return null;
    }

    private Map<String, Object> oracleColumnRow(ResultSet rs) throws SQLException {
        return Map.of(
            "name", rs.getString("column_name"),
            "type", rs.getString("data_type"),
            "nullable", !"N".equalsIgnoreCase(rs.getString("nullable")),
            "position", rs.getInt("column_id")
        );
    }

    private boolean isOracleSystemOwner(String owner) {
        return owner != null && ORACLE_SYSTEM_OWNER_SET.contains(owner.toUpperCase(Locale.ROOT));
    }

    private Response getPrimaryKeys(Request req) throws SQLException {
        int connId    = getInt(req, "conn-id");
        String schema = (String) req.params.get("schema");
        String table  = getString(req, "table");
        Connection conn = connMgr.get(connId);
        List<String> pks = isOracle(conn)
            ? getOraclePrimaryKeys(conn, schema, table)
            : getJdbcMetadataPrimaryKeys(conn, schema, table);
        return Response.ok(req.id, Map.of("primary-keys", pks));
    }

    private List<String> getJdbcMetadataPrimaryKeys(Connection conn, String schema, String table)
            throws SQLException {
        List<String> pks = new ArrayList<>();
        try (ResultSet rs = conn.getMetaData().getPrimaryKeys(null, schema, table)) {
            while (rs.next()) pks.add(rs.getString("COLUMN_NAME"));
        }
        return pks;
    }

    private List<String> getOraclePrimaryKeys(Connection conn, String schema, String table)
            throws SQLException {
        OracleSchema os = OracleSchema.of(currentOracleUser(conn), schema);
        String sql;
        if (os.useUserTables()) {
            sql = """
                SELECT ucc.column_name
                FROM user_constraints uc
                JOIN user_cons_columns ucc ON uc.constraint_name = ucc.constraint_name
                WHERE uc.constraint_type = 'P'
                  AND uc.table_name = ?
                ORDER BY ucc.position
                """;
        } else {
            sql = """
                SELECT acc.column_name
                FROM all_constraints ac
                JOIN all_cons_columns acc ON ac.constraint_name = acc.constraint_name
                                         AND ac.owner = acc.owner
                WHERE ac.constraint_type = 'P'
                  AND ac.owner = ?
                  AND ac.table_name = ?
                ORDER BY acc.position
                """;
        }
        List<String> pks = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setQueryTimeout(ORACLE_METADATA_TIMEOUT_SECONDS);
            int i = 1;
            if (!os.useUserTables()) {
                ps.setString(i++, os.owner());
            }
            ps.setString(i, table.toUpperCase(Locale.ROOT));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) pks.add(rs.getString("column_name"));
            }
        }
        return pks;
    }

    private Response getForeignKeys(Request req) throws SQLException {
        int connId    = getInt(req, "conn-id");
        String schema = (String) req.params.get("schema");
        String table  = getString(req, "table");
        Connection conn = connMgr.get(connId);
        List<Map<String, Object>> fks = isOracle(conn)
            ? getOracleForeignKeys(conn, schema, table)
            : getJdbcMetadataForeignKeys(conn, schema, table);
        return Response.ok(req.id, Map.of("foreign-keys", fks));
    }

    private List<Map<String, Object>> getJdbcMetadataForeignKeys(Connection conn, String schema, String table)
            throws SQLException {
        List<Map<String, Object>> fks = new ArrayList<>();
        try (ResultSet rs = conn.getMetaData().getImportedKeys(null, schema, table)) {
            while (rs.next()) {
                fks.add(Map.of(
                    "fk-column",   rs.getString("FKCOLUMN_NAME"),
                    "pk-table",    rs.getString("PKTABLE_NAME"),
                    "pk-schema",   Objects.toString(rs.getString("PKTABLE_SCHEM"), ""),
                    "pk-column",   rs.getString("PKCOLUMN_NAME")
                ));
            }
        }
        return fks;
    }

    private List<Map<String, Object>> getOracleForeignKeys(Connection conn, String schema, String table)
            throws SQLException {
        OracleSchema os = OracleSchema.of(currentOracleUser(conn), schema);
        String sql;
        if (os.useUserTables()) {
            sql = """
                SELECT ucc.column_name AS fk_column,
                       ruc.table_name AS pk_table,
                       ruc.owner AS pk_schema,
                       rucc.column_name AS pk_column
                FROM user_constraints uc
                JOIN user_cons_columns ucc ON uc.constraint_name = ucc.constraint_name
                JOIN all_constraints ruc ON uc.r_constraint_name = ruc.constraint_name
                JOIN all_cons_columns rucc ON ruc.constraint_name = rucc.constraint_name
                                          AND ruc.owner = rucc.owner
                                          AND ucc.position = rucc.position
                WHERE uc.constraint_type = 'R'
                  AND uc.table_name = ?
                ORDER BY ucc.position
                """;
        } else {
            sql = """
                SELECT acc.column_name AS fk_column,
                       ruc.table_name AS pk_table,
                       ruc.owner AS pk_schema,
                       rucc.column_name AS pk_column
                FROM all_constraints ac
                JOIN all_cons_columns acc ON ac.constraint_name = acc.constraint_name
                                         AND ac.owner = acc.owner
                JOIN all_constraints ruc ON ac.r_constraint_name = ruc.constraint_name
                JOIN all_cons_columns rucc ON ruc.constraint_name = rucc.constraint_name
                                          AND ruc.owner = rucc.owner
                                          AND acc.position = rucc.position
                WHERE ac.constraint_type = 'R'
                  AND ac.owner = ?
                  AND ac.table_name = ?
                ORDER BY acc.position
                """;
        }
        List<Map<String, Object>> fks = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setQueryTimeout(ORACLE_METADATA_TIMEOUT_SECONDS);
            int i = 1;
            if (!os.useUserTables()) {
                ps.setString(i++, os.owner());
            }
            ps.setString(i, table.toUpperCase(Locale.ROOT));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    fks.add(Map.of(
                        "fk-column",  rs.getString("fk_column"),
                        "pk-table",   rs.getString("pk_table"),
                        "pk-schema",  Objects.toString(rs.getString("pk_schema"), ""),
                        "pk-column",  rs.getString("pk_column")
                    ));
                }
            }
        }
        return fks;
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

    /**
     * Captures the two decisions derived from a schema argument in Oracle methods:
     * whether to query user_* views (vs all_*), and what owner string to bind.
     */
    private record OracleSchema(boolean useUserTables, String owner) {
        static OracleSchema of(String currentUser, String schema) {
            boolean hasSchema = schema != null && !schema.isBlank();
            boolean currentUserSchema = hasSchema && schema.equalsIgnoreCase(currentUser);
            boolean useUserTables = !hasSchema || currentUserSchema;
            String owner = useUserTables
                ? Objects.toString(currentUser, Objects.toString(schema, ""))
                : schema.toUpperCase(Locale.ROOT);
            return new OracleSchema(useUserTables, owner);
        }
    }

    private record OracleObject(String owner, String name) {}
}
