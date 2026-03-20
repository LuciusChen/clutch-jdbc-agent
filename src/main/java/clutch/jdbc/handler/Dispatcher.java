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
            case "set-current-schema" -> setCurrentSchema(req);
            case "get-tables"      -> getTables(req);
            case "search-tables"   -> searchTables(req);
            case "get-columns"     -> getColumns(req);
            case "search-columns"  -> searchColumns(req);
            case "get-primary-keys"-> getPrimaryKeys(req);
            case "get-foreign-keys"-> getForeignKeys(req);
            case "get-referencing-objects" -> getReferencingObjects(req);
            case "get-indexes"     -> getIndexes(req);
            case "get-index-columns" -> getIndexColumns(req);
            case "get-sequences"   -> getSequences(req);
            case "get-procedures"  -> getProcedures(req);
            case "get-functions"   -> getFunctions(req);
            case "get-procedure-params" -> getProcedureParams(req);
            case "get-function-params" -> getFunctionParams(req);
            case "get-object-source" -> getObjectSource(req);
            case "get-object-ddl"  -> getObjectDdl(req);
            case "get-triggers"    -> getTriggers(req);
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
        primaryConnection(connId).commit();
        return Response.ok(req.id, Map.of("conn-id", connId));
    }

    private Response rollback(Request req) throws SQLException {
        int connId = getInt(req, "conn-id");
        primaryConnection(connId).rollback();
        return Response.ok(req.id, Map.of("conn-id", connId));
    }

    private Response setAutoCommit(Request req) throws SQLException {
        int connId = getInt(req, "conn-id");
        Object autoCommitValue = req.params.get("auto-commit");
        boolean autoCommit = autoCommitValue == null || Boolean.TRUE.equals(autoCommitValue);
        primaryConnection(connId).setAutoCommit(autoCommit);
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

        Connection conn = primaryConnection(connId);
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
        DatabaseMetaData meta = metadataConnection(connId).getMetaData();
        List<String> schemas = new ArrayList<>();
        try (ResultSet rs = meta.getSchemas()) {
            while (rs.next()) schemas.add(rs.getString("TABLE_SCHEM"));
        }
        return Response.ok(req.id, Map.of("schemas", schemas));
    }

    private Response setCurrentSchema(Request req) throws SQLException {
        int connId = getInt(req, "conn-id");
        String schema = getString(req, "schema");
        Connection primary = primaryConnection(connId);
        Connection metadata = metadataConnection(connId);
        applyCurrentSchema(primary, schema);
        if (metadata != primary) {
            applyCurrentSchema(metadata, schema);
        }
        return Response.ok(req.id, Map.of("conn-id", connId, "schema", schema));
    }

    private Response getTables(Request req) throws SQLException {
        int connId    = getInt(req, "conn-id");
        String schema = (String) req.params.get("schema");
        Connection conn = metadataConnection(connId);
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

        Connection conn = metadataConnection(connId);
        List<Map<String, Object>> cols = isOracle(conn)
            ? getOracleColumns(conn, schema, table, null)
            : getJdbcMetadataColumns(conn, schema, table, null);
        return Response.ok(req.id, Map.of("columns", cols));
    }

    private Response searchTables(Request req) throws SQLException {
        int connId      = getInt(req, "conn-id");
        String schema   = (String) req.params.get("schema");
        String prefix   = Objects.toString(req.params.get("prefix"), "");
        Connection conn = metadataConnection(connId);
        List<Map<String, Object>> tables = isOracle(conn)
            ? searchOracleTables(conn, schema, prefix)
            : searchJdbcMetadataTables(conn, schema, prefix);
        return Response.ok(req.id, Map.of("tables", tables));
    }

    private List<Map<String, Object>> searchJdbcMetadataTables(Connection conn, String schema, String prefix)
            throws SQLException {
        String[] types = { "TABLE", "VIEW", "SYNONYM" };
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
                  UNION ALL
                  SELECT synonym_name AS object_name, 'SYNONYM' AS object_type, owner
                  FROM all_synonyms
                  WHERE owner = ? AND synonym_name LIKE ?
                  UNION ALL
                  SELECT synonym_name AS object_name, 'SYNONYM' AS object_type, owner
                  FROM all_synonyms
                  WHERE owner = 'PUBLIC' AND synonym_name LIKE ?
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
        Connection conn = metadataConnection(connId);
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
        Connection conn = metadataConnection(connId);
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
        Connection conn = metadataConnection(connId);
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

    private Response getReferencingObjects(Request req) throws SQLException {
        int connId    = getInt(req, "conn-id");
        String schema = (String) req.params.get("schema");
        String table  = getString(req, "table");
        Connection conn = metadataConnection(connId);
        List<Map<String, Object>> objects = isOracle(conn)
            ? getOracleReferencingObjects(conn, schema, table)
            : getJdbcMetadataReferencingObjects(conn, schema, table);
        return Response.ok(req.id, Map.of("objects", objects));
    }

    private List<Map<String, Object>> getJdbcMetadataReferencingObjects(Connection conn, String schema, String table)
            throws SQLException {
        List<Map<String, Object>> objects = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        try (ResultSet rs = conn.getMetaData().getExportedKeys(null, schema, table)) {
            while (rs.next()) {
                String name = rs.getString("FKTABLE_NAME");
                if (name == null || !seen.add(Objects.toString(rs.getString("FKTABLE_SCHEM"), "") + "\u0000" + name)) {
                    continue;
                }
                objects.add(Map.of(
                    "name", name,
                    "schema", Objects.toString(rs.getString("FKTABLE_SCHEM"), "")
                ));
            }
        }
        return objects;
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

    private List<Map<String, Object>> getOracleReferencingObjects(Connection conn, String schema, String table)
            throws SQLException {
        OracleSchema os = OracleSchema.of(currentOracleUser(conn), schema);
        String sql;
        if (os.useUserTables()) {
            sql = """
                SELECT DISTINCT ac.table_name AS name,
                                USER AS schema
                FROM user_constraints uc
                JOIN user_constraints ac
                  ON ac.r_constraint_name = uc.constraint_name
                WHERE uc.constraint_type IN ('P', 'U')
                  AND uc.table_name = ?
                  AND ac.constraint_type = 'R'
                ORDER BY ac.table_name
                """;
        } else {
            sql = """
                SELECT DISTINCT ac.table_name AS name,
                                ac.owner AS schema
                FROM all_constraints uc
                JOIN all_constraints ac
                  ON ac.r_constraint_name = uc.constraint_name
                 AND ac.r_owner = uc.owner
                WHERE uc.owner = ?
                  AND uc.constraint_type IN ('P', 'U')
                  AND uc.table_name = ?
                  AND ac.owner = ?
                  AND ac.constraint_type = 'R'
                ORDER BY ac.owner, ac.table_name
                """;
        }
        List<Map<String, Object>> objects = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setQueryTimeout(ORACLE_METADATA_TIMEOUT_SECONDS);
            int i = 1;
            if (!os.useUserTables()) {
                ps.setString(i++, os.owner());
            }
            ps.setString(i, table.toUpperCase(Locale.ROOT));
            if (!os.useUserTables()) {
                ps.setString(++i, os.owner());
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    objects.add(Map.of(
                        "name", rs.getString("name"),
                        "schema", Objects.toString(rs.getString("schema"), "")
                    ));
                }
            }
        }
        return objects;
    }

    private Response getIndexes(Request req) throws SQLException {
        int connId    = getInt(req, "conn-id");
        String schema = (String) req.params.get("schema");
        String table  = getOptionalString(req, "table");
        Connection conn = metadataConnection(connId);
        List<Map<String, Object>> indexes = isOracle(conn)
            ? getOracleIndexes(conn, schema, table)
            : getJdbcIndexes(conn, schema, table);
        return Response.ok(req.id, Map.of("indexes", indexes));
    }

    private List<Map<String, Object>> getJdbcMetadataTables(Connection conn, String schema)
            throws SQLException {
        String[] types = { "TABLE", "VIEW", "SYNONYM" };
        DatabaseMetaData meta = conn.getMetaData();
        List<Map<String, Object>> tables = new ArrayList<>();
        try (ResultSet rs = meta.getTables(null, schema, "%", types)) {
            while (rs.next()) {
                String tableSchema = Objects.toString(rs.getString("TABLE_SCHEM"), "");
                tables.add(entryMap(
                    "name", rs.getString("TABLE_NAME"),
                    "type", rs.getString("TABLE_TYPE"),
                    "schema", tableSchema,
                    "source-schema", tableSchema
                ));
            }
        }
        return tables;
    }

    private List<Map<String, Object>> getOracleIndexes(Connection conn, String schema, String table)
            throws SQLException {
        OracleSchema os = OracleSchema.of(currentOracleUser(conn), schema);
        String sql;
        if (os.useUserTables()) {
            sql = """
                SELECT index_name, table_name, uniqueness
                FROM user_indexes
                WHERE index_type IN ('NORMAL', 'BITMAP', 'FUNCTION-BASED NORMAL')
                  AND (? IS NULL OR table_name = ?)
                ORDER BY table_name, index_name
                """;
        } else {
            sql = """
                SELECT index_name, table_name, uniqueness, owner
                FROM all_indexes
                WHERE owner = ?
                  AND index_type IN ('NORMAL', 'BITMAP', 'FUNCTION-BASED NORMAL')
                  AND (? IS NULL OR table_name = ?)
                ORDER BY table_name, index_name
                """;
        }
        List<Map<String, Object>> indexes = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setQueryTimeout(ORACLE_METADATA_TIMEOUT_SECONDS);
            int i = 1;
            if (!os.useUserTables()) {
                ps.setString(i++, os.owner());
            }
            String tableName = table == null ? null : table.toUpperCase(Locale.ROOT);
            ps.setString(i++, tableName);
            ps.setString(i, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    indexes.add(entryMap(
                        "name", rs.getString("index_name"),
                        "type", "INDEX",
                        "schema", os.owner(),
                        "source-schema", os.owner(),
                        "table", rs.getString("table_name"),
                        "unique", "UNIQUE".equalsIgnoreCase(rs.getString("uniqueness"))
                    ));
                }
            }
        }
        return indexes;
    }

    private List<Map<String, Object>> getJdbcIndexes(Connection conn, String schema, String table)
            throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        List<Map<String, Object>> indexes = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        List<String> tables = table == null
            ? getJdbcMetadataTables(conn, schema).stream()
                .filter(entry -> "TABLE".equalsIgnoreCase(Objects.toString(entry.get("type"), "")))
                .map(entry -> Objects.toString(entry.get("name"), ""))
                .toList()
            : List.of(table);
        for (String tableName : tables) {
            try (ResultSet rs = meta.getIndexInfo(null, schema, tableName, false, true)) {
                while (rs.next()) {
                    short type = rs.getShort("TYPE");
                    String indexName = rs.getString("INDEX_NAME");
                    if (type == DatabaseMetaData.tableIndexStatistic || indexName == null) {
                        continue;
                    }
                    String key = tableName + "\u0000" + indexName;
                    if (seen.add(key)) {
                        indexes.add(entryMap(
                            "name", indexName,
                            "type", "INDEX",
                            "schema", schema,
                            "source-schema", schema,
                            "table", rs.getString("TABLE_NAME"),
                            "unique", !rs.getBoolean("NON_UNIQUE")
                        ));
                    }
                }
            }
        }
        return indexes;
    }

    private Response getIndexColumns(Request req) throws SQLException {
        int connId    = getInt(req, "conn-id");
        String schema = (String) req.params.get("schema");
        String index  = getString(req, "index");
        String table  = getOptionalString(req, "table");
        Connection conn = metadataConnection(connId);
        List<Map<String, Object>> columns = isOracle(conn)
            ? getOracleIndexColumns(conn, schema, index)
            : getJdbcIndexColumns(conn, schema, index, table);
        return Response.ok(req.id, Map.of("columns", columns));
    }

    private List<Map<String, Object>> getOracleIndexColumns(Connection conn, String schema, String index)
            throws SQLException {
        OracleSchema os = OracleSchema.of(currentOracleUser(conn), schema);
        String sql;
        if (os.useUserTables()) {
            sql = """
                SELECT column_name, column_position, descend
                FROM user_ind_columns
                WHERE index_name = ?
                ORDER BY column_position
                """;
        } else {
            sql = """
                SELECT column_name, column_position, descend
                FROM all_ind_columns
                WHERE index_owner = ?
                  AND index_name = ?
                ORDER BY column_position
                """;
        }
        List<Map<String, Object>> columns = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setQueryTimeout(ORACLE_METADATA_TIMEOUT_SECONDS);
            int i = 1;
            if (!os.useUserTables()) {
                ps.setString(i++, os.owner());
            }
            ps.setString(i, index.toUpperCase(Locale.ROOT));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    columns.add(entryMap(
                        "name", rs.getString("column_name"),
                        "position", rs.getInt("column_position"),
                        "descend", rs.getString("descend")
                    ));
                }
            }
        }
        return columns;
    }

    private List<Map<String, Object>> getJdbcIndexColumns(Connection conn, String schema,
                                                          String index, String table)
            throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        List<Map<String, Object>> columns = new ArrayList<>();
        List<String> tables = table == null
            ? getJdbcIndexes(conn, schema, null).stream()
                .filter(entry -> index.equalsIgnoreCase(Objects.toString(entry.get("name"), "")))
                .map(entry -> Objects.toString(entry.get("table"), ""))
                .filter(name -> !name.isBlank())
                .distinct()
                .toList()
            : List.of(table);
        for (String tableName : tables) {
            try (ResultSet rs = meta.getIndexInfo(null, schema, tableName, false, true)) {
                while (rs.next()) {
                    short type = rs.getShort("TYPE");
                    String indexName = rs.getString("INDEX_NAME");
                    if (type == DatabaseMetaData.tableIndexStatistic
                        || indexName == null
                        || !index.equalsIgnoreCase(indexName)) {
                        continue;
                    }
                    columns.add(entryMap(
                        "name", rs.getString("COLUMN_NAME"),
                        "position", rs.getInt("ORDINAL_POSITION"),
                        "descend", switch (Objects.toString(rs.getString("ASC_OR_DESC"), "")) {
                            case "D" -> "DESC";
                            default -> "ASC";
                        }
                    ));
                }
            }
        }
        columns.sort(Comparator.comparingInt(col -> ((Number) col.get("position")).intValue()));
        return columns;
    }

    private Response getSequences(Request req) throws SQLException {
        int connId    = getInt(req, "conn-id");
        String schema = (String) req.params.get("schema");
        Connection conn = metadataConnection(connId);
        List<Map<String, Object>> sequences = isOracle(conn)
            ? getOracleSequences(conn, schema)
            : getDialectSequences(conn, schema);
        return Response.ok(req.id, Map.of("sequences", sequences));
    }

    private List<Map<String, Object>> getOracleSequences(Connection conn, String schema)
            throws SQLException {
        OracleSchema os = OracleSchema.of(currentOracleUser(conn), schema);
        String sql;
        if (os.useUserTables()) {
            sql = """
                SELECT sequence_name, min_value, max_value, increment_by, last_number
                FROM user_sequences
                ORDER BY sequence_name
                """;
        } else {
            sql = """
                SELECT sequence_name, min_value, max_value, increment_by, last_number
                FROM all_sequences
                WHERE sequence_owner = ?
                ORDER BY sequence_name
                """;
        }
        List<Map<String, Object>> sequences = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setQueryTimeout(ORACLE_METADATA_TIMEOUT_SECONDS);
            if (!os.useUserTables()) {
                ps.setString(1, os.owner());
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    sequences.add(entryMap(
                        "name", rs.getString("sequence_name"),
                        "type", "SEQUENCE",
                        "schema", os.owner(),
                        "source-schema", os.owner(),
                        "min", rs.getObject("min_value"),
                        "max", rs.getObject("max_value"),
                        "increment", rs.getObject("increment_by"),
                        "last", rs.getObject("last_number")
                    ));
                }
            }
        }
        return sequences;
    }

    private List<Map<String, Object>> getDialectSequences(Connection conn, String schema)
            throws SQLException {
        String product = conn.getMetaData().getDatabaseProductName().toLowerCase(Locale.ROOT);
        if (product.contains("postgres")) {
            String effectiveSchema = (schema == null || schema.isBlank()) ? "current_schema()" : "?";
            String sql = """
                SELECT sequencename, min_value, max_value, increment_by, last_value
                FROM pg_sequences
                WHERE schemaname = %s
                ORDER BY sequencename
                """.formatted(effectiveSchema);
            List<Map<String, Object>> sequences = new ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setQueryTimeout(ORACLE_METADATA_TIMEOUT_SECONDS);
                if (!"current_schema()".equals(effectiveSchema)) {
                    ps.setString(1, schema);
                }
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        sequences.add(entryMap(
                            "name", rs.getString("sequencename"),
                            "type", "SEQUENCE",
                            "schema", schema,
                            "source-schema", schema,
                            "min", rs.getObject("min_value"),
                            "max", rs.getObject("max_value"),
                            "increment", rs.getObject("increment_by"),
                            "last", rs.getObject("last_value")
                        ));
                    }
                }
            }
            return sequences;
        }
        return List.of();
    }

    private Response getProcedures(Request req) throws SQLException {
        int connId    = getInt(req, "conn-id");
        String schema = (String) req.params.get("schema");
        Connection conn = metadataConnection(connId);
        List<Map<String, Object>> procedures = isOracle(conn)
            ? getOracleRoutines(conn, schema, "PROCEDURE")
            : getJdbcRoutines(conn, schema, true);
        return Response.ok(req.id, Map.of("procedures", procedures));
    }

    private Response getFunctions(Request req) throws SQLException {
        int connId    = getInt(req, "conn-id");
        String schema = (String) req.params.get("schema");
        Connection conn = metadataConnection(connId);
        List<Map<String, Object>> functions = isOracle(conn)
            ? getOracleRoutines(conn, schema, "FUNCTION")
            : getJdbcRoutines(conn, schema, false);
        return Response.ok(req.id, Map.of("functions", functions));
    }

    private List<Map<String, Object>> getOracleRoutines(Connection conn, String schema, String type)
            throws SQLException {
        OracleSchema os = OracleSchema.of(currentOracleUser(conn), schema);
        String sql;
        if (os.useUserTables()) {
            sql = """
                SELECT object_name, status
                FROM user_objects
                WHERE object_type = ?
                ORDER BY object_name
                """;
        } else {
            sql = """
                SELECT object_name, status
                FROM all_objects
                WHERE owner = ?
                  AND object_type = ?
                ORDER BY object_name
                """;
        }
        List<Map<String, Object>> routines = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setQueryTimeout(ORACLE_METADATA_TIMEOUT_SECONDS);
            int i = 1;
            if (!os.useUserTables()) {
                ps.setString(i++, os.owner());
            }
            ps.setString(i, type);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    routines.add(entryMap(
                        "name", rs.getString("object_name"),
                        "type", type,
                        "schema", os.owner(),
                        "source-schema", os.owner(),
                        "status", rs.getString("status")
                    ));
                }
            }
        }
        return routines;
    }

    private List<Map<String, Object>> getJdbcRoutines(Connection conn, String schema, boolean procedures)
            throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        List<Map<String, Object>> routines = new ArrayList<>();
        try (ResultSet rs = procedures
                ? meta.getProcedures(null, schema, "%")
                : meta.getFunctions(null, schema, "%")) {
            while (rs.next()) {
                String name = procedures
                    ? rs.getString("PROCEDURE_NAME")
                    : rs.getString("FUNCTION_NAME");
                String specificName = rs.getString("SPECIFIC_NAME");
                routines.add(entryMap(
                    "name", name,
                    "type", procedures ? "PROCEDURE" : "FUNCTION",
                    "schema", Objects.toString(schema, ""),
                    "source-schema", Objects.toString(schema, ""),
                    "identity", specificName == null ? null : "SPECIFIC_NAME:" + specificName
                ));
            }
        }
        return routines;
    }

    private Response getProcedureParams(Request req) throws SQLException {
        int connId     = getInt(req, "conn-id");
        String schema  = (String) req.params.get("schema");
        String name    = getString(req, "name");
        String identity = getOptionalString(req, "identity");
        Connection conn = metadataConnection(connId);
        List<Map<String, Object>> params = isOracle(conn)
            ? getOracleRoutineParams(conn, schema, name)
            : getJdbcRoutineParams(conn, schema, name, identity, true);
        return Response.ok(req.id, Map.of("params", params));
    }

    private Response getFunctionParams(Request req) throws SQLException {
        int connId     = getInt(req, "conn-id");
        String schema  = (String) req.params.get("schema");
        String name    = getString(req, "name");
        String identity = getOptionalString(req, "identity");
        Connection conn = metadataConnection(connId);
        List<Map<String, Object>> params = isOracle(conn)
            ? getOracleRoutineParams(conn, schema, name)
            : getJdbcRoutineParams(conn, schema, name, identity, false);
        return Response.ok(req.id, Map.of("params", params));
    }

    private List<Map<String, Object>> getOracleRoutineParams(Connection conn, String schema, String name)
            throws SQLException {
        OracleSchema os = OracleSchema.of(currentOracleUser(conn), schema);
        String sql;
        if (os.useUserTables()) {
            sql = """
                SELECT argument_name, data_type, in_out, position
                FROM user_arguments
                WHERE object_name = ?
                  AND package_name IS NULL
                ORDER BY position
                """;
        } else {
            sql = """
                SELECT argument_name, data_type, in_out, position
                FROM all_arguments
                WHERE owner = ?
                  AND object_name = ?
                  AND package_name IS NULL
                ORDER BY position
                """;
        }
        List<Map<String, Object>> params = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setQueryTimeout(ORACLE_METADATA_TIMEOUT_SECONDS);
            int i = 1;
            if (!os.useUserTables()) {
                ps.setString(i++, os.owner());
            }
            ps.setString(i, name.toUpperCase(Locale.ROOT));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    int position = rs.getInt("position");
                    params.add(entryMap(
                        "name", rs.getString("argument_name"),
                        "type", rs.getString("data_type"),
                        "mode", position == 0 ? "RETURN" : rs.getString("in_out"),
                        "position", position
                    ));
                }
            }
        }
        return params;
    }

    private List<Map<String, Object>> getJdbcRoutineParams(Connection conn, String schema, String name,
                                                           String identity, boolean procedures)
            throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        List<Map<String, Object>> params = new ArrayList<>();
        String specificName = specificNameFromIdentity(identity);
        try (ResultSet rs = procedures
                ? meta.getProcedureColumns(null, schema, name, "%")
                : meta.getFunctionColumns(null, schema, name, "%")) {
            while (rs.next()) {
                if (specificName != null
                    && !specificName.equals(Objects.toString(rs.getString("SPECIFIC_NAME"), ""))) {
                    continue;
                }
                int columnType = rs.getInt("COLUMN_TYPE");
                params.add(entryMap(
                    "name", rs.getString("COLUMN_NAME"),
                    "type", rs.getString("TYPE_NAME"),
                    "mode", routinesColumnMode(columnType, procedures),
                    "position", rs.getInt("ORDINAL_POSITION")
                ));
            }
        }
        return params;
    }

    private Response getObjectSource(Request req) throws SQLException {
        int connId      = getInt(req, "conn-id");
        String schema   = (String) req.params.get("schema");
        String name     = getString(req, "name");
        String type     = getString(req, "type");
        String identity = getOptionalString(req, "identity");
        Connection conn = metadataConnection(connId);
        String source = isOracle(conn)
            ? getOracleObjectSource(conn, schema, name, type)
            : getDialectObjectSource(conn, schema, name, type, identity);
        return Response.ok(req.id, entryMap("source", source));
    }

    private Response getObjectDdl(Request req) throws SQLException {
        int connId      = getInt(req, "conn-id");
        String schema   = (String) req.params.get("schema");
        String name     = getString(req, "name");
        String type     = getString(req, "type");
        String identity = getOptionalString(req, "identity");
        Connection conn = metadataConnection(connId);
        String ddl = isOracle(conn)
            ? getOracleObjectDdl(conn, schema, name, type)
            : getDialectObjectDdl(conn, schema, name, type, identity);
        return Response.ok(req.id, entryMap("ddl", ddl));
    }

    private String getOracleObjectSource(Connection conn, String schema, String name, String type)
            throws SQLException {
        OracleSchema os = OracleSchema.of(currentOracleUser(conn), schema);
        String sql;
        if (os.useUserTables()) {
            sql = """
                SELECT text
                FROM user_source
                WHERE name = ? AND type = ?
                ORDER BY line
                """;
        } else {
            sql = """
                SELECT text
                FROM all_source
                WHERE owner = ? AND name = ? AND type = ?
                ORDER BY line
                """;
        }
        StringBuilder source = new StringBuilder();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setQueryTimeout(ORACLE_METADATA_TIMEOUT_SECONDS);
            int i = 1;
            if (!os.useUserTables()) {
                ps.setString(i++, os.owner());
            }
            ps.setString(i++, name.toUpperCase(Locale.ROOT));
            ps.setString(i, type.toUpperCase(Locale.ROOT));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    source.append(Objects.toString(rs.getString("text"), ""));
                }
            }
        }
        return source.length() == 0 ? null : source.toString();
    }

    private String getOracleObjectDdl(Connection conn, String schema, String name, String type)
            throws SQLException {
        OracleSchema os = OracleSchema.of(currentOracleUser(conn), schema);
        try (PreparedStatement ps =
                 conn.prepareStatement("SELECT DBMS_METADATA.GET_DDL(?, ?, ?) FROM DUAL")) {
            ps.setQueryTimeout(ORACLE_METADATA_TIMEOUT_SECONDS);
            ps.setString(1, type.toUpperCase(Locale.ROOT));
            ps.setString(2, name.toUpperCase(Locale.ROOT));
            ps.setString(3, os.owner());
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return readLargeText(rs, 1);
            }
        }
    }

    private String getDialectObjectSource(Connection conn, String schema, String name,
                                          String type, String identity)
            throws SQLException {
        String product = conn.getMetaData().getDatabaseProductName().toLowerCase(Locale.ROOT);
        if (!product.contains("postgres")) {
            return null;
        }
        String oid = oidFromIdentity(identity);
        if (oid == null) {
            return null;
        }
        String sql = switch (type.toUpperCase(Locale.ROOT)) {
            case "PROCEDURE", "FUNCTION" ->
                "SELECT pg_get_functiondef(?::oid)";
            case "TRIGGER" ->
                "SELECT pg_get_triggerdef(?::oid, true)";
            default -> null;
        };
        if (sql == null) {
            return null;
        }
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, oid);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }

    private String getDialectObjectDdl(Connection conn, String schema, String name,
                                       String type, String identity)
            throws SQLException {
        String product = conn.getMetaData().getDatabaseProductName().toLowerCase(Locale.ROOT);
        if (!product.contains("postgres")) {
            return null;
        }
        return switch (type.toUpperCase(Locale.ROOT)) {
            case "INDEX" -> querySingleString(
                conn,
                "SELECT pg_get_indexdef(i.oid) " +
                    "FROM pg_class i JOIN pg_namespace n ON n.oid = i.relnamespace " +
                    "WHERE i.relkind = 'i' AND i.relname = ? AND n.nspname = current_schema()",
                name);
            case "VIEW" -> querySingleString(
                conn,
                "SELECT 'CREATE OR REPLACE VIEW ' || quote_ident(viewname) || E' AS\\n' || " +
                    "pg_get_viewdef((quote_ident(schemaname) || ''.'' || quote_ident(viewname))::regclass, true) " +
                    "FROM pg_views WHERE schemaname = current_schema() AND viewname = ?",
                name);
            case "SEQUENCE" -> querySingleString(
                conn,
                "SELECT format('CREATE SEQUENCE %I.%I INCREMENT BY %s MINVALUE %s MAXVALUE %s START WITH %s;', " +
                    "schemaname, sequencename, increment_by, min_value, max_value, start_value) " +
                    "FROM pg_sequences WHERE schemaname = current_schema() AND sequencename = ?",
                name);
            case "TRIGGER" -> getDialectObjectSource(conn, schema, name, type, identity);
            default -> null;
        };
    }

    private String querySingleString(Connection conn, String sql, String value) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, value);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }

    private String readLargeText(ResultSet rs, int column) throws SQLException {
        Clob clob = rs.getClob(column);
        if (clob != null) {
            try (java.io.Reader reader = clob.getCharacterStream()) {
                StringBuilder text = new StringBuilder();
                char[] buffer = new char[4096];
                int n;
                while ((n = reader.read(buffer)) != -1) {
                    text.append(buffer, 0, n);
                }
                return text.toString();
            } catch (java.io.IOException e) {
                throw new SQLException("Failed reading CLOB", e);
            }
        }
        return rs.getString(column);
    }

    private Response getTriggers(Request req) throws SQLException {
        int connId    = getInt(req, "conn-id");
        String schema = (String) req.params.get("schema");
        String table  = getOptionalString(req, "table");
        Connection conn = metadataConnection(connId);
        List<Map<String, Object>> triggers = isOracle(conn)
            ? getOracleTriggers(conn, schema, table)
            : getDialectTriggers(conn, schema, table);
        return Response.ok(req.id, Map.of("triggers", triggers));
    }

    private List<Map<String, Object>> getOracleTriggers(Connection conn, String schema, String table)
            throws SQLException {
        OracleSchema os = OracleSchema.of(currentOracleUser(conn), schema);
        String sql;
        if (os.useUserTables()) {
            sql = """
                SELECT trigger_name, table_name, triggering_event, trigger_type, status
                FROM user_triggers
                WHERE (? IS NULL OR table_name = ?)
                ORDER BY table_name, trigger_name
                """;
        } else {
            sql = """
                SELECT trigger_name, table_name, triggering_event, trigger_type, status
                FROM all_triggers
                WHERE owner = ?
                  AND (? IS NULL OR table_name = ?)
                ORDER BY table_name, trigger_name
                """;
        }
        List<Map<String, Object>> triggers = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setQueryTimeout(ORACLE_METADATA_TIMEOUT_SECONDS);
            int i = 1;
            if (!os.useUserTables()) {
                ps.setString(i++, os.owner());
            }
            String tableName = table == null ? null : table.toUpperCase(Locale.ROOT);
            ps.setString(i++, tableName);
            ps.setString(i, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    triggers.add(entryMap(
                        "name", rs.getString("trigger_name"),
                        "type", "TRIGGER",
                        "schema", os.owner(),
                        "source-schema", os.owner(),
                        "table", rs.getString("table_name"),
                        "event", rs.getString("triggering_event"),
                        "timing", rs.getString("trigger_type"),
                        "status", rs.getString("status")
                    ));
                }
            }
        }
        return triggers;
    }

    private List<Map<String, Object>> getDialectTriggers(Connection conn, String schema, String table)
            throws SQLException {
        String product = conn.getMetaData().getDatabaseProductName().toLowerCase(Locale.ROOT);
        if (product.contains("postgres")) {
            return getPostgresTriggers(conn, schema, table);
        }
        if (product.contains("mysql")) {
            return getMySqlTriggers(conn, schema, table);
        }
        return List.of();
    }

    private List<Map<String, Object>> getPostgresTriggers(Connection conn, String schema, String table)
            throws SQLException {
        String effectiveSchema = (schema == null || schema.isBlank()) ? "current_schema()" : "?";
        String sql = """
            SELECT t.trigger_name, t.event_object_table, t.event_manipulation,
                   t.action_timing, pg_t.oid
            FROM information_schema.triggers t
            JOIN pg_class c ON c.relname = t.event_object_table
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_trigger pg_t ON pg_t.tgrelid = c.oid
                                AND pg_t.tgname = t.trigger_name
            WHERE t.trigger_schema = %s
              AND NOT pg_t.tgisinternal
              AND (? IS NULL OR t.event_object_table = ?)
            ORDER BY t.event_object_table, t.trigger_name
            """.formatted(effectiveSchema);
        Map<String, Map<String, Object>> grouped = new LinkedHashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            int i = 1;
            if (!"current_schema()".equals(effectiveSchema)) {
                ps.setString(i++, schema);
            }
            ps.setString(i++, table);
            ps.setString(i, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String key = rs.getString("trigger_name") + "\u0000" + rs.getString("event_object_table");
                    Map<String, Object> trigger = grouped.get(key);
                    if (trigger == null) {
                        trigger = entryMap(
                            "name", rs.getString("trigger_name"),
                            "type", "TRIGGER",
                            "schema", schema,
                            "source-schema", schema,
                            "table", rs.getString("event_object_table"),
                            "timing", rs.getString("action_timing"),
                            "status", "ENABLED",
                            "identity", "OID:" + rs.getLong("oid"),
                            "event", rs.getString("event_manipulation")
                        );
                        grouped.put(key, trigger);
                    }
                    String existing = Objects.toString(trigger.get("event"), "");
                    String event = rs.getString("event_manipulation");
                    if (!existing.contains(event)) {
                        trigger.put("event", existing.isBlank() ? event : existing + " OR " + event);
                    }
                }
            }
        }
        return new ArrayList<>(grouped.values());
    }

    private List<Map<String, Object>> getMySqlTriggers(Connection conn, String schema, String table)
            throws SQLException {
        String sql = """
            SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE, EVENT_MANIPULATION, ACTION_TIMING
            FROM INFORMATION_SCHEMA.TRIGGERS
            WHERE TRIGGER_SCHEMA = DATABASE()
              AND (? IS NULL OR EVENT_OBJECT_TABLE = ?)
            ORDER BY EVENT_OBJECT_TABLE, TRIGGER_NAME
            """;
        List<Map<String, Object>> triggers = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, table);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    triggers.add(entryMap(
                        "name", rs.getString("TRIGGER_NAME"),
                        "type", "TRIGGER",
                        "schema", schema,
                        "source-schema", schema,
                        "table", rs.getString("EVENT_OBJECT_TABLE"),
                        "event", rs.getString("EVENT_MANIPULATION"),
                        "timing", rs.getString("ACTION_TIMING"),
                        "status", "ENABLED"
                    ));
                }
            }
        }
        return triggers;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private Connection primaryConnection(int connId) throws SQLException {
        return connMgr.getPrimary(connId);
    }

    private Connection metadataConnection(int connId) throws SQLException {
        return connMgr.getMetadata(connId);
    }

    private void applyCurrentSchema(Connection conn, String schema) throws SQLException {
        String trimmed = schema.strip();
        if (trimmed.isEmpty()) {
            throw new SQLException("Schema must not be blank");
        }
        if (isOracle(conn)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("ALTER SESSION SET CURRENT_SCHEMA = "
                    + quoteIdentifier(trimmed, "oracle"));
            }
            return;
        }
        String productName = conn.getMetaData().getDatabaseProductName();
        if (productName != null
            && productName.toLowerCase(Locale.ROOT).contains("mysql")) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("USE " + quoteIdentifier(trimmed, "mysql"));
            }
            return;
        }
        throw new SQLException("Runtime schema switching is not available for this connection");
    }

    private String quoteIdentifier(String identifier, String product) {
        if ("mysql".equals(product)) {
            return "`" + identifier.replace("`", "``") + "`";
        }
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private Map<String, Object> entryMap(Object... kvs) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < kvs.length; i += 2) {
            Object value = kvs[i + 1];
            if (value != null) {
                map.put((String) kvs[i], value);
            }
        }
        return map;
    }

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

    private String getOptionalString(Request req, String key) {
        Object v = req.params.get(key);
        if (v == null) return null;
        if (v instanceof String s) return s;
        throw new IllegalArgumentException("Non-string param: " + key);
    }

    private Integer getOptionalInt(Request req, String key) {
        Object v = req.params.get(key);
        if (v == null) return null;
        if (v instanceof Number n) return n.intValue();
        throw new IllegalArgumentException("Non-integer param: " + key);
    }

    private String routinesColumnMode(int columnType, boolean procedures) {
        if (columnType == DatabaseMetaData.procedureColumnIn
            || columnType == DatabaseMetaData.functionColumnIn) {
            return "IN";
        }
        if (columnType == DatabaseMetaData.procedureColumnInOut
            || columnType == DatabaseMetaData.functionColumnInOut) {
            return "INOUT";
        }
        if (columnType == DatabaseMetaData.procedureColumnOut
            || columnType == DatabaseMetaData.functionColumnOut) {
            return "OUT";
        }
        if (columnType == DatabaseMetaData.procedureColumnReturn
            || columnType == DatabaseMetaData.functionReturn
            || columnType == DatabaseMetaData.functionColumnResult) {
            return "RETURN";
        }
        return "IN";
    }

    private String specificNameFromIdentity(String identity) {
        if (identity == null || !identity.startsWith("SPECIFIC_NAME:")) {
            return null;
        }
        return identity.substring("SPECIFIC_NAME:".length());
    }

    private String oidFromIdentity(String identity) {
        if (identity == null || !identity.startsWith("OID:")) {
            return null;
        }
        return identity.substring("OID:".length());
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
