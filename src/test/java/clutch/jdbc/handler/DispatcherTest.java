package clutch.jdbc.handler;

import clutch.jdbc.ConnectionManager;
import clutch.jdbc.CursorManager;
import clutch.jdbc.model.Request;
import clutch.jdbc.model.Response;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.DatabaseMetaData;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DispatcherTest {

    @Test
    void connectForwardsExplicitTimeouts() throws Exception {
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        Response response = dispatch(connMgr, 1, "connect",
            "url", "jdbc:test:demo",
            "user", "scott",
            "password", "tiger",
            "props", Map.of("role", "reporting"),
            "connect-timeout-seconds", 8,
            "network-timeout-seconds", 9,
            "auto-commit", false);

        assertTrue(response.ok);
        assertEquals("jdbc:test:demo", connMgr.url);
        assertEquals("scott", connMgr.user);
        assertEquals("tiger", connMgr.password);
        assertEquals(Map.of("role", "reporting"), connMgr.props);
        assertEquals(8, connMgr.connectTimeoutSeconds);
        assertEquals(9, connMgr.networkTimeoutSeconds);
        assertFalse(connMgr.autoCommit);
        assertEquals(42, ((Number) ((Map<?, ?>) response.result).get("conn-id")).intValue());
    }

    @Test
    void commitCallsConnectionCommit() throws Exception {
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        boolean[] committed = {false};
        connMgr.connection = connectionWithNoArgCall("commit", () -> committed[0] = true);
        Response response = dispatch(connMgr, 7, "commit", "conn-id", 7);

        assertTrue(response.ok);
        assertTrue(committed[0]);
    }

    @Test
    void rollbackCallsConnectionRollback() throws Exception {
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        boolean[] rolledBack = {false};
        connMgr.connection = connectionWithNoArgCall("rollback", () -> rolledBack[0] = true);
        Response response = dispatch(connMgr, 8, "rollback", "conn-id", 7);

        assertTrue(response.ok);
        assertTrue(rolledBack[0]);
    }

    @Test
    void setAutoCommitTrueCallsConnectionSetAutoCommit() throws Exception {
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        Boolean[] capturedValue = {null};
        connMgr.connection = connectionWithBooleanCall("setAutoCommit", value -> capturedValue[0] = value);
        Response response = dispatch(connMgr, 9, "set-auto-commit",
            "conn-id", 7,
            "auto-commit", true);

        assertTrue(response.ok);
        assertTrue(capturedValue[0]);
        assertEquals(true, resultMap(response).get("auto-commit"));
    }

    @Test
    void setAutoCommitFalseCallsConnectionSetAutoCommit() throws Exception {
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        Boolean[] capturedValue = {null};
        connMgr.connection = connectionWithBooleanCall("setAutoCommit", value -> capturedValue[0] = value);
        Response response = dispatch(connMgr, 10, "set-auto-commit",
            "conn-id", 7,
            "auto-commit", false);

        assertTrue(response.ok);
        assertFalse(capturedValue[0]);
        assertEquals(false, resultMap(response).get("auto-commit"));
    }

    @Test
    void getSchemasUsesMetadataConnection() throws Exception {
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        connMgr.connection = (Connection) Proxy.newProxyInstance(
            DispatcherTest.class.getClassLoader(),
            new Class<?>[]{Connection.class},
            (_proxy, method, _args) -> switch (method.getName()) {
                case "getMetaData" -> throw new AssertionError("primary connection should not serve metadata");
                case "unwrap" -> null;
                case "isWrapperFor" -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });
        connMgr.metadataConnection = metadataConnectionWithSchemas(List.of("APP", "REPORTING"));
        Response response = dispatch(connMgr, 11, "get-schemas", "conn-id", 7);

        assertTrue(response.ok);
        assertEquals(List.of("APP", "REPORTING"), resultMap(response).get("schemas"));
    }

    @Test
    void setCurrentSchemaUpdatesPrimaryAndMetadataConnections() throws Exception {
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        List<String> primarySql = new ArrayList<>();
        List<String> metadataSql = new ArrayList<>();
        connMgr.connection = oracleSchemaConnection(primarySql);
        connMgr.metadataConnection = oracleSchemaConnection(metadataSql);
        Response response = dispatch(connMgr, 12, "set-current-schema",
            "conn-id", 7,
            "schema", "CJH_TEST");

        assertTrue(response.ok);
        assertEquals(List.of("ALTER SESSION SET CURRENT_SCHEMA = \"CJH_TEST\""), primarySql);
        assertEquals(List.of("ALTER SESSION SET CURRENT_SCHEMA = \"CJH_TEST\""), metadataSql);
        assertEquals("CJH_TEST", resultMap(response).get("schema"));
    }

    @Test
    void executeAppliesQueryTimeoutBeforeRunningStatement() throws Exception {
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        RecordingStatementHandler stmt = new RecordingStatementHandler();
        connMgr.connection = proxyConnection(stmt);
        Response response = dispatch(connMgr, 2, "execute",
            "conn-id", 7,
            "sql", "update demo set x = 1;",
            "query-timeout-seconds", 16);

        assertTrue(response.ok);
        assertEquals(16, stmt.queryTimeoutSeconds);
        assertEquals("update demo set x = 1", stmt.executedSql);
        assertTrue(stmt.closed);
        assertEquals("dml", resultMap(response).get("type"));
        assertEquals(3, ((Number) resultMap(response).get("affected-rows")).intValue());
    }

    @Test
    void executeClosesStatementWhenResultSetFails() throws Exception {
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        boolean[] closed = {false};
        Statement failingStmt = (Statement) Proxy.newProxyInstance(
            DispatcherTest.class.getClassLoader(),
            new Class<?>[]{Statement.class},
            (_proxy, method, args) -> switch (method.getName()) {
                case "setQueryTimeout" -> null;
                case "execute"         -> true;  // pretend it's a SELECT
                case "getResultSet"    -> throw new java.sql.SQLException("simulated rs failure");
                case "close"           -> { closed[0] = true; yield null; }
                case "unwrap"          -> null;
                case "isWrapperFor"    -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });
        connMgr.connection = (Connection) Proxy.newProxyInstance(
            DispatcherTest.class.getClassLoader(),
            new Class<?>[]{Connection.class},
            (_proxy, method, _args) -> switch (method.getName()) {
                case "isValid"         -> true;
                case "createStatement" -> failingStmt;
                case "unwrap"          -> null;
                case "isWrapperFor"    -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });
        assertThrows(java.sql.SQLException.class,
            () -> dispatch(connMgr, 99, "execute", "conn-id", 7, "sql", "SELECT 1"));
        assertTrue(closed[0], "statement must be closed when getResultSet() fails");
    }

    @Test
    void executeReturnsErrorWhenConnectionIsInvalid() throws Exception {
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        connMgr.connection = (Connection) Proxy.newProxyInstance(
            DispatcherTest.class.getClassLoader(),
            new Class<?>[]{Connection.class},
            (_proxy, method, _args) -> switch (method.getName()) {
                case "isValid"      -> false;   // dead connection
                case "unwrap"       -> null;
                case "isWrapperFor" -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });
        Response response = dispatch(connMgr, 19, "execute",
            "conn-id", 7,
            "sql", "SELECT 1 FROM dual");

        assertFalse(response.ok);
        assertNotNull(response.error);
        assertTrue(response.error.contains("idle timeout"), "error should mention idle timeout: " + response.error);
    }

    @Test
    void executeTimesOutAndCancelsStatementWhenBlocked() throws Exception {
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        boolean[] cancelled = {false};
        boolean[] closed    = {false};
        Statement blockingStmt = (Statement) Proxy.newProxyInstance(
            DispatcherTest.class.getClassLoader(),
            new Class<?>[]{Statement.class},
            (_proxy, method, args) -> switch (method.getName()) {
                case "setQueryTimeout" -> null;
                case "execute" -> {
                    try { Thread.sleep(Long.MAX_VALUE); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    throw new java.sql.SQLException("interrupted");
                }
                case "cancel" -> { cancelled[0] = true; yield null; }
                case "close"  -> { closed[0] = true; yield null; }
                case "unwrap" -> null;
                case "isWrapperFor" -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });
        connMgr.connection = (Connection) Proxy.newProxyInstance(
            DispatcherTest.class.getClassLoader(),
            new Class<?>[]{Connection.class},
            (_proxy, method, _args) -> switch (method.getName()) {
                case "isValid"         -> true;
                case "createStatement" -> blockingStmt;
                case "unwrap"          -> null;
                case "isWrapperFor"    -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });
        long start = System.currentTimeMillis();
        Response response = dispatch(connMgr, 20, "execute",
            "conn-id", 7,
            "sql", "UPDATE t SET x = 1",
            "query-timeout-seconds", 1);
        long elapsed = System.currentTimeMillis() - start;

        assertFalse(response.ok);
        assertNotNull(response.error);
        assertTrue(response.error.contains("timed out"), "error should mention timed out: " + response.error);
        assertTrue(cancelled[0], "stmt.cancel() must be called on timeout");
        assertTrue(closed[0],    "stmt.close() must be called on timeout");
        assertTrue(elapsed < 4000, "test must complete within 4s, took: " + elapsed + "ms");
    }

    @Test
    void executeUsesDefaultTimeoutWhenNoneSpecified() throws Exception {
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        int[] capturedTimeout = {-1};
        Statement timedStmt = (Statement) Proxy.newProxyInstance(
            DispatcherTest.class.getClassLoader(),
            new Class<?>[]{Statement.class},
            (_proxy, method, args) -> switch (method.getName()) {
                case "setQueryTimeout" -> { capturedTimeout[0] = (Integer) args[0]; yield null; }
                case "execute"         -> false;   // DML
                case "getUpdateCount"  -> 0;
                case "close"           -> null;
                case "unwrap"          -> null;
                case "isWrapperFor"    -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });
        connMgr.connection = (Connection) Proxy.newProxyInstance(
            DispatcherTest.class.getClassLoader(),
            new Class<?>[]{Connection.class},
            (_proxy, method, _args) -> switch (method.getName()) {
                case "isValid"         -> true;
                case "createStatement" -> timedStmt;
                case "unwrap"          -> null;
                case "isWrapperFor"    -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });
        Response response = dispatch(connMgr, 21, "execute",
            "conn-id", 7,
            "sql", "DELETE FROM t WHERE 1=0");

        assertTrue(response.ok);
        assertEquals(Dispatcher.DEFAULT_EXECUTE_TIMEOUT, capturedTimeout[0],
            "default timeout should be used when none specified");
        assertEquals("dml", resultMap(response).get("type"));
    }

    @Test
    void getColumnsUsesOracleFastPath() throws Exception {
        OracleMetadataRecorder oracle = new OracleMetadataRecorder();
        Response response = dispatchOracle(oracle, 3, "get-columns",
            "conn-id", 7,
            "schema", "ZJSY",
            "table", "t_sys_para");

        assertTrue(response.ok);
        assertEquals(
            "SELECT column_name, data_type, nullable, column_id\n" +
            "FROM user_tab_columns\n" +
            "WHERE table_name = ?\n" +
            "  AND column_name LIKE ?\n" +
            "ORDER BY column_id",
            oracle.lastSqlNormalized());
        assertEquals(List.of("T_SYS_PARA", "%"), oracle.params);
        assertEquals(5, oracle.queryTimeoutSeconds);
        List<Map<String, Object>> cols = resultList(response, "columns");
        assertEquals("PARA_ID", cols.get(0).get("name"));
    }

    @Test
    void getTablesIncludesOracleSynonyms() throws Exception {
        OracleMetadataRecorder oracle = new OracleMetadataRecorder();
        oracle.resultRows = List.of(
            oracle.objectRow("USER_SYM", "SYNONYM", "APP", "ZJSY")
        );
        Response response = dispatchOracle(oracle, 30, "get-tables",
            "conn-id", 7,
            "schema", "ZJSY");

        assertTrue(response.ok);
        assertTrue(oracle.lastSqlNormalized().contains("user_synonyms"));
        assertEquals(List.of("ZJSY", "ZJSY", "ZJSY", "ZJSY", "ZJSY", "ZJSY", "ZJSY"), oracle.params);
        List<List<Object>> rows = resultList(response, "rows");
        assertEquals(List.of("USER_SYM", "SYNONYM", "APP", "ZJSY"), rows.get(0));
    }

    @Test
    void searchColumnsUsesOracleFastPathAndPrefix() throws Exception {
        OracleMetadataRecorder oracle = new OracleMetadataRecorder();
        Response response = dispatchOracle(oracle, 4, "search-columns",
            "conn-id", 7,
            "schema", "ZJSY",
            "table", "t_sys_para",
            "prefix", "pa");

        assertTrue(response.ok);
        assertEquals(List.of("T_SYS_PARA", "PA%"), oracle.params);
        List<Map<String, Object>> cols = resultList(response, "columns");
        assertEquals(2, cols.size());
        assertEquals("PARA_NAME", cols.get(1).get("name"));
    }

    @Test
    void searchTablesUsesOracleAccessibleObjectPath() throws Exception {
        OracleMetadataRecorder oracle = new OracleMetadataRecorder();
        oracle.resultRows = List.of(
            oracle.objectRow("ORDERS", "SYNONYM", "DATA_OWNER", "ZJSY"),
            oracle.objectRow("ORDER_LOG", "TABLE", "REPORTING")
        );
        Response response = dispatchOracle(oracle, 40, "search-tables",
            "conn-id", 7,
            "schema", "ZJSY",
            "prefix", "or");

        assertTrue(response.ok);
        assertTrue(oracle.lastSqlNormalized().contains("user_synonyms"),
            "search-tables should consult user synonyms for current-user schema");
        assertTrue(oracle.lastSqlNormalized().contains("all_tables"),
            "search-tables should consult accessible tables outside the current schema");
        assertTrue(oracle.lastSqlNormalized().toUpperCase().contains("OWNER NOT IN ('SYS', 'SYSTEM', 'XDB'"),
            "search-tables should filter Oracle system owners in SQL");
        assertEquals(List.of("ZJSY", "ZJSY", "OR%",
                             "ZJSY", "ZJSY", "OR%",
                             "ZJSY", "OR%",
                             "ZJSY", "OR%",
                             "ZJSY", "OR%",
                             "OR%"),
            oracle.params);
        List<Map<String, Object>> tables = resultList(response, "tables");
        assertEquals(2, tables.size());
        assertEquals("ORDERS", tables.get(0).get("name"));
        assertEquals("DATA_OWNER", tables.get(0).get("schema"));
        assertEquals("ZJSY", tables.get(0).get("source-schema"));
    }

    @Test
    void searchTablesKeepsOraclePublicSynonymsForSystemOwners() throws Exception {
        OracleMetadataRecorder oracle = new OracleMetadataRecorder();
        oracle.resultRows = List.of(
            oracle.objectRow("USER_TABLES", "PUBLIC SYNONYM", "SYS", "PUBLIC")
        );
        Response response = dispatchOracle(oracle, 42, "search-tables",
            "conn-id", 7,
            "schema", "APP",
            "prefix", "user_");

        assertTrue(response.ok);
        List<Map<String, Object>> tables = resultList(response, "tables");
        assertEquals(1, tables.size());
        assertEquals("USER_TABLES", tables.get(0).get("name"));
        assertEquals("PUBLIC SYNONYM", tables.get(0).get("type"));
        assertEquals("SYS", tables.get(0).get("schema"));
        assertEquals("PUBLIC", tables.get(0).get("source-schema"));
    }

    @Test
    void getTablesUsesOracleAccessibleObjectPath() throws Exception {
        OracleMetadataRecorder oracle = new OracleMetadataRecorder();
        oracle.resultRows = List.of(
            oracle.objectRow("CUSTOMERS", "SYNONYM", "DATA_OWNER", "ZJSY"),
            oracle.objectRow("ORDERS", "SYNONYM", "DATA_OWNER", "ZJSY"),
            oracle.objectRow("PAYMENTS", "TABLE", "DATA_OWNER", "DATA_OWNER")
        );
        Response response = dispatchOracle(oracle, 43, "get-tables",
            "conn-id", 7,
            "schema", "ZJSY");

        assertTrue(response.ok);
        assertTrue(oracle.executedSqlsNormalized().stream()
                       .anyMatch(sql -> sql.contains("user_synonyms")),
            "get-tables should include user synonyms for current-user schema");
        assertTrue(oracle.executedSqlsNormalized().stream()
                       .anyMatch(sql -> sql.contains("all_tables")),
            "get-tables should include accessible tables outside the current schema");
        assertEquals(List.of("ZJSY", "ZJSY", "ZJSY", "ZJSY", "ZJSY", "ZJSY", "ZJSY"), oracle.params);
        List<List<Object>> rows = resultList(response, "rows");
        assertEquals(3, rows.size());
        assertEquals(List.of("CUSTOMERS", "SYNONYM", "DATA_OWNER", "ZJSY"), rows.get(0));
        assertEquals(List.of("ORDERS", "SYNONYM", "DATA_OWNER", "ZJSY"), rows.get(1));
        assertEquals(List.of("PAYMENTS", "TABLE", "DATA_OWNER", "DATA_OWNER"), rows.get(2));
    }

    @Test
    void searchColumnsFallsBackToResolvedOracleSynonym() throws Exception {
        OracleMetadataRecorder oracle = new OracleMetadataRecorder();
        oracle.setRowsForSql("""
            SELECT column_name, data_type, nullable, column_id
            FROM user_tab_columns
            WHERE table_name = ?
              AND column_name LIKE ?
            ORDER BY column_id
            """, List.of());
        oracle.setRowsForSql("""
            SELECT table_owner, table_name
            FROM user_synonyms
            WHERE synonym_name = ?
            UNION ALL
            SELECT table_owner, table_name
            FROM all_synonyms
            WHERE owner = 'PUBLIC'
              AND synonym_name = ?
            """, List.of(oracle.synonymRow("DATA_OWNER", "ORDERS")));
        oracle.setRowsForSql("""
            SELECT column_name, data_type, nullable, column_id
            FROM all_tab_columns
            WHERE owner = ?
              AND table_name = ?
              AND column_name LIKE ?
            ORDER BY column_id
            """, List.of(
                oracle.row("PARA_ID", "NUMBER", "N", 1),
                oracle.row("PARA_NAME", "VARCHAR2", "Y", 2)
            ));
        Response response = dispatchOracle(oracle, 41, "search-columns",
            "conn-id", 7,
            "schema", "ZJSY",
            "table", "orders",
            "prefix", "pa");

        assertTrue(response.ok);
        assertTrue(oracle.executedSqlsNormalized().stream()
                       .anyMatch(sql -> sql.contains("user_synonyms")),
            "search-columns should resolve the synonym before loading columns");
        assertEquals(
            "SELECT column_name, data_type, nullable, column_id\n" +
            "FROM all_tab_columns\n" +
            "WHERE owner = ?\n" +
            "  AND table_name = ?\n" +
            "  AND column_name LIKE ?\n" +
            "ORDER BY column_id",
            oracle.lastSqlNormalized());
        assertEquals(List.of("DATA_OWNER", "ORDERS", "PA%"), oracle.params);
        List<Map<String, Object>> cols = resultList(response, "columns");
        assertEquals(2, cols.size());
        assertEquals("PARA_ID", cols.get(0).get("name"));
        assertEquals("PARA_NAME", cols.get(1).get("name"));
    }

    @Test
    void getPrimaryKeysUsesOracleFastPath() throws Exception {
        OracleMetadataRecorder oracle = new OracleMetadataRecorder();
        // default resultRows have "column_name" → "PARA_ID" / "PARA_NAME", which the PK query reads
        Response response = dispatchOracle(oracle, 5, "get-primary-keys",
            "conn-id", 7,
            "schema", "ZJSY",
            "table", "orders");

        assertTrue(response.ok);
        assertTrue(oracle.lastSqlNormalized().contains("user_constraints"),
            "should use Oracle fast path (user_constraints)");
        assertEquals(List.of("ORDERS"), oracle.params);
        assertEquals(5, oracle.queryTimeoutSeconds);
        List<String> pks = resultList(response, "primary-keys");
        assertEquals(List.of("PARA_ID", "PARA_NAME"), pks);
    }

    @Test
    void getForeignKeysUsesOracleFastPath() throws Exception {
        OracleMetadataRecorder oracle = new OracleMetadataRecorder();
        Map<String, Object> fkRow = new HashMap<>();
        fkRow.put("fk_column", "CUSTOMER_ID");
        fkRow.put("pk_table",  "CUSTOMERS");
        fkRow.put("pk_schema", "ZJSY");
        fkRow.put("pk_column", "ID");
        oracle.resultRows = List.of(fkRow);
        Response response = dispatchOracle(oracle, 6, "get-foreign-keys",
            "conn-id", 7,
            "schema", "ZJSY",
            "table", "orders");

        assertTrue(response.ok);
        assertTrue(oracle.lastSqlNormalized().contains("user_constraints"),
            "should use Oracle fast path (user_constraints)");
        assertEquals(List.of("ORDERS"), oracle.params);
        assertEquals(5, oracle.queryTimeoutSeconds);
        List<Map<?, ?>> fks = resultList(response, "foreign-keys");
        assertEquals(1, fks.size());
        assertEquals("CUSTOMER_ID", fks.get(0).get("fk-column"));
        assertEquals("CUSTOMERS",   fks.get(0).get("pk-table"));
        assertEquals("ZJSY",        fks.get(0).get("pk-schema"));
        assertEquals("ID",          fks.get(0).get("pk-column"));
    }

    @Test
    void getReferencingObjectsUsesOracleFastPath() throws Exception {
        OracleMetadataRecorder oracle = new OracleMetadataRecorder();
        Map<String, Object> row = new HashMap<>();
        row.put("name", "ORDERS");
        row.put("schema", "ZJSY");
        oracle.resultRows = List.of(row);
        Response response = dispatchOracle(oracle, 33, "get-referencing-objects",
            "conn-id", 7,
            "schema", "ZJSY",
            "table", "customers");

        assertTrue(response.ok);
        assertTrue(oracle.lastSqlNormalized().contains("user_constraints"));
        assertEquals(List.of("CUSTOMERS"), oracle.params);
        List<Map<String, Object>> objects = resultList(response, "objects");
        assertEquals("ORDERS", objects.get(0).get("name"));
        assertEquals("ZJSY", objects.get(0).get("schema"));
    }

    @Test
    void getIndexesUsesOracleFastPath() throws Exception {
        OracleMetadataRecorder oracle = new OracleMetadataRecorder();
        Map<String, Object> indexRow = new HashMap<>();
        indexRow.put("index_name", "ORDER_IDX");
        indexRow.put("table_name", "ORDERS");
        indexRow.put("uniqueness", "UNIQUE");
        oracle.resultRows = List.of(indexRow);
        Response response = dispatchOracle(oracle, 31, "get-indexes",
            "conn-id", 7,
            "schema", "ZJSY",
            "table", "orders");

        assertTrue(response.ok);
        assertTrue(oracle.lastSqlNormalized().contains("user_indexes"));
        assertEquals(List.of("ORDERS", "ORDERS"), oracle.params);
        List<Map<String, Object>> indexes = resultList(response, "indexes");
        assertEquals("ORDER_IDX", indexes.get(0).get("name"));
        assertEquals("ORDERS", indexes.get(0).get("table"));
        assertEquals(true, indexes.get(0).get("unique"));
    }

    @Test
    void getProceduresUsesOracleObjectsFastPath() throws Exception {
        OracleMetadataRecorder oracle = new OracleMetadataRecorder();
        Map<String, Object> procRow = new HashMap<>();
        procRow.put("object_name", "PROCESS_ORDER");
        procRow.put("status", "VALID");
        oracle.resultRows = List.of(procRow);
        Response response = dispatchOracle(oracle, 32, "get-procedures",
            "conn-id", 7,
            "schema", "ZJSY");

        assertTrue(response.ok);
        assertTrue(oracle.lastSqlNormalized().contains("user_objects"));
        assertEquals(List.of("PROCEDURE"), oracle.params);
        List<Map<String, Object>> procedures = resultList(response, "procedures");
        assertEquals("PROCESS_ORDER", procedures.get(0).get("name"));
        assertEquals("VALID", procedures.get(0).get("status"));
    }

    private static Response dispatch(RecordingConnectionManager connMgr, int id, String op,
                                     Object... params) throws Exception {
        return new Dispatcher(connMgr, new CursorManager()).dispatch(request(id, op, params));
    }

    private static Response dispatchOracle(OracleMetadataRecorder oracle, int id, String op,
                                           Object... params) throws Exception {
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        connMgr.connection = oracle.connection();
        return dispatch(connMgr, id, op, params);
    }

    private static Request request(int id, String op, Object... params) {
        if (params.length % 2 != 0) {
            throw new IllegalArgumentException("params must contain key/value pairs");
        }
        Request req = new Request();
        req.id = id;
        req.op = op;
        for (int i = 0; i < params.length; i += 2) {
            req.params.put((String) params[i], params[i + 1]);
        }
        return req;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> resultMap(Response response) {
        return (Map<String, Object>) response.result;
    }

    @SuppressWarnings("unchecked")
    private static <T> List<T> resultList(Response response, String key) {
        return (List<T>) resultMap(response).get(key);
    }

    private static Connection connectionWithNoArgCall(String methodName, Runnable callback) {
        return (Connection) Proxy.newProxyInstance(
            DispatcherTest.class.getClassLoader(),
            new Class<?>[]{Connection.class},
            (_proxy, method, _args) -> {
                if (method.getName().equals(methodName)) {
                    callback.run();
                    return null;
                }
                if (method.getName().equals("unwrap")) {
                    return null;
                }
                if (method.getName().equals("isWrapperFor")) {
                    return false;
                }
                throw new UnsupportedOperationException(method.getName());
            });
    }

    private static Connection connectionWithBooleanCall(String methodName, Consumer<Boolean> callback) {
        return (Connection) Proxy.newProxyInstance(
            DispatcherTest.class.getClassLoader(),
            new Class<?>[]{Connection.class},
            (_proxy, method, args) -> {
                if (method.getName().equals(methodName)) {
                    callback.accept((Boolean) args[0]);
                    return null;
                }
                if (method.getName().equals("unwrap")) {
                    return null;
                }
                if (method.getName().equals("isWrapperFor")) {
                    return false;
                }
                throw new UnsupportedOperationException(method.getName());
            });
    }

    private static Connection proxyConnection(RecordingStatementHandler stmt) {
        return (Connection) Proxy.newProxyInstance(
            DispatcherTest.class.getClassLoader(),
            new Class<?>[]{Connection.class},
            (_proxy, method, _args) -> switch (method.getName()) {
                case "isValid"         -> true;
                case "createStatement" -> stmt.proxy();
                case "unwrap"          -> null;
                case "isWrapperFor"    -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });
    }

    private static Connection metadataConnectionWithSchemas(List<String> schemas) {
        DatabaseMetaData meta = (DatabaseMetaData) Proxy.newProxyInstance(
            DispatcherTest.class.getClassLoader(),
            new Class<?>[]{DatabaseMetaData.class},
            (_proxy, method, _args) -> switch (method.getName()) {
                case "getSchemas" -> resultSet(List.of("TABLE_SCHEM"), schemas.stream()
                    .map(schema -> List.<Object>of(schema))
                    .toList());
                case "unwrap" -> null;
                case "isWrapperFor" -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });
        return (Connection) Proxy.newProxyInstance(
            DispatcherTest.class.getClassLoader(),
            new Class<?>[]{Connection.class},
            (_proxy, method, _args) -> switch (method.getName()) {
                case "getMetaData" -> meta;
                case "unwrap" -> null;
                case "isWrapperFor" -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });
    }

    private static Connection oracleSchemaConnection(List<String> executedSql) {
        DatabaseMetaData meta = (DatabaseMetaData) Proxy.newProxyInstance(
            DispatcherTest.class.getClassLoader(),
            new Class<?>[]{DatabaseMetaData.class},
            (_proxy, method, _args) -> switch (method.getName()) {
                case "getDatabaseProductName" -> "Oracle";
                case "unwrap" -> null;
                case "isWrapperFor" -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });
        Statement stmt = (Statement) Proxy.newProxyInstance(
            DispatcherTest.class.getClassLoader(),
            new Class<?>[]{Statement.class},
            (_proxy, method, args) -> switch (method.getName()) {
                case "execute" -> {
                    executedSql.add((String) args[0]);
                    yield true;
                }
                case "close" -> null;
                case "unwrap" -> null;
                case "isWrapperFor" -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });
        return (Connection) Proxy.newProxyInstance(
            DispatcherTest.class.getClassLoader(),
            new Class<?>[]{Connection.class},
            (_proxy, method, _args) -> switch (method.getName()) {
                case "getMetaData" -> meta;
                case "createStatement" -> stmt;
                case "unwrap" -> null;
                case "isWrapperFor" -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });
    }

    private static ResultSet resultSet(List<String> columns, List<List<Object>> rows) {
        final int[] index = {-1};
        return (ResultSet) Proxy.newProxyInstance(
            DispatcherTest.class.getClassLoader(),
            new Class<?>[]{ResultSet.class},
            (_proxy, method, args) -> switch (method.getName()) {
                case "next" -> ++index[0] < rows.size();
                case "getString" -> {
                    Object key = args[0];
                    int col = key instanceof Integer n
                        ? n.intValue() - 1
                        : columns.indexOf((String) key);
                    Object value = rows.get(index[0]).get(col);
                    yield value == null ? null : value.toString();
                }
                case "close" -> null;
                case "unwrap" -> null;
                case "isWrapperFor" -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });
    }

    private static final class RecordingConnectionManager extends ConnectionManager {
        private String url;
        private String user;
        private String password;
        private Map<String, String> props;
        private Integer connectTimeoutSeconds;
        private Integer networkTimeoutSeconds;
        private boolean autoCommit = true;
        private Connection connection;
        private Connection metadataConnection;

        @Override
        public int connect(String url, String user, String password, Map<String, String> props,
                           Integer connectTimeoutSeconds, Integer networkTimeoutSeconds,
                           boolean autoCommit) {
            this.url = url;
            this.user = user;
            this.password = password;
            this.props = props;
            this.connectTimeoutSeconds = connectTimeoutSeconds;
            this.networkTimeoutSeconds = networkTimeoutSeconds;
            this.autoCommit = autoCommit;
            return 42;
        }

        @Override
        public Connection get(int connId) {
            return getPrimary(connId);
        }

        @Override
        public Connection getPrimary(int connId) {
            assertEquals(7, connId);
            return connection;
        }

        @Override
        public Connection getMetadata(int connId) {
            assertEquals(7, connId);
            return metadataConnection != null ? metadataConnection : connection;
        }
    }

    private static final class RecordingStatementHandler {
        private int queryTimeoutSeconds;
        private String executedSql;
        private boolean closed;

        private Statement proxy() {
            return (Statement) Proxy.newProxyInstance(
                DispatcherTest.class.getClassLoader(),
                new Class<?>[]{Statement.class},
                (_proxy, method, args) -> switch (method.getName()) {
                    case "setQueryTimeout" -> {
                        queryTimeoutSeconds = (Integer) args[0];
                        yield null;
                    }
                    case "execute" -> {
                        executedSql = (String) args[0];
                        yield false;
                    }
                    case "getUpdateCount" -> 3;
                    case "close" -> {
                        closed = true;
                        yield null;
                    }
                    case "unwrap" -> null;
                    case "isWrapperFor" -> false;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        }
    }

    private static final class OracleMetadataRecorder {
        private String sql;
        private final List<String> sqlHistory = new ArrayList<>();
        private final List<String> params = new ArrayList<>();
        private int queryTimeoutSeconds;
        private final Map<String, List<Map<String, Object>>> rowsBySql = new HashMap<>();
        List<Map<String, Object>> resultRows = List.of(
            row("PARA_ID", "NUMBER", "N", 1),
            row("PARA_NAME", "VARCHAR2", "Y", 2)
        );

        private Connection connection() {
            DatabaseMetaData meta = (DatabaseMetaData) Proxy.newProxyInstance(
                DispatcherTest.class.getClassLoader(),
                new Class<?>[]{DatabaseMetaData.class},
                (_proxy, method, _args) -> switch (method.getName()) {
                    case "getDatabaseProductName" -> "Oracle";
                    case "getUserName" -> "zjsy";
                    case "unwrap" -> null;
                    case "isWrapperFor" -> false;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
            return (Connection) Proxy.newProxyInstance(
                DispatcherTest.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (_proxy, method, _args) -> switch (method.getName()) {
                    case "getMetaData" -> meta;
                    case "prepareStatement" -> preparedStatement((String) _args[0]);
                    case "unwrap" -> null;
                    case "isWrapperFor" -> false;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        }

        private PreparedStatement preparedStatement(String sql) {
            this.sql = sql;
            this.sqlHistory.add(sql);
            this.params.clear();
            return (PreparedStatement) Proxy.newProxyInstance(
                DispatcherTest.class.getClassLoader(),
                new Class<?>[]{PreparedStatement.class},
                (_proxy, method, args) -> switch (method.getName()) {
                    case "setQueryTimeout" -> {
                        queryTimeoutSeconds = (Integer) args[0];
                        yield null;
                    }
                    case "setString" -> {
                        int idx = (Integer) args[0];
                        while (params.size() < idx) params.add(null);
                        params.set(idx - 1, (String) args[1]);
                        yield null;
                    }
                    case "executeQuery" -> resultSet();
                    case "close" -> null;
                    case "unwrap" -> null;
                    case "isWrapperFor" -> false;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        }

        private ResultSet resultSet() {
            List<Map<String, Object>> rows = rowsBySql.getOrDefault(lastSqlNormalized(), this.resultRows);
            List<String> columnLabels = resultSetColumnLabels(rows);
            return (ResultSet) Proxy.newProxyInstance(
                DispatcherTest.class.getClassLoader(),
                new Class<?>[]{ResultSet.class},
                new java.lang.reflect.InvocationHandler() {
                    int index = -1;
                    Object lastValue;
                    @Override
                    public Object invoke(Object _proxy, java.lang.reflect.Method method, Object[] _args) {
                        return switch (method.getName()) {
                            case "next" -> ++index < rows.size();
                            case "getString" -> {
                                if (_args[0] instanceof Integer col) {
                                    lastValue = resultSetValue(rows.get(index), columnLabels.get(col - 1));
                                    yield lastValue;
                                }
                                lastValue = rows.get(index).get((String) _args[0]);
                                yield lastValue;
                            }
                            case "getInt" -> rows.get(index).get((String) _args[0]);
                            case "getObject" -> {
                                if (_args[0] instanceof Integer col) {
                                    lastValue = resultSetValue(rows.get(index), columnLabels.get(col - 1));
                                    yield lastValue;
                                }
                                lastValue = rows.get(index).get((String) _args[0]);
                                yield lastValue;
                            }
                            case "wasNull" -> lastValue == null;
                            case "getMetaData" -> resultSetMetaData(columnLabels);
                            case "setFetchSize" -> null;
                            case "close" -> null;
                            case "unwrap" -> null;
                            case "isWrapperFor" -> false;
                            default -> throw new UnsupportedOperationException(method.getName());
                        };
                    }
                });
        }

        private ResultSetMetaData resultSetMetaData(List<String> columnLabels) {
            return (ResultSetMetaData) Proxy.newProxyInstance(
                DispatcherTest.class.getClassLoader(),
                new Class<?>[]{ResultSetMetaData.class},
                (_proxy, method, args) -> switch (method.getName()) {
                    case "getColumnCount" -> columnLabels.size();
                    case "getColumnLabel" -> columnLabels.get(((Integer) args[0]) - 1);
                    case "getColumnTypeName" -> "VARCHAR";
                    case "unwrap" -> null;
                    case "isWrapperFor" -> false;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        }

        private List<String> resultSetColumnLabels(List<Map<String, Object>> rows) {
            if (rows.isEmpty()) return List.of();
            Map<String, Object> row = rows.get(0);
            if (row.containsKey("object_name")) return List.of("name", "type", "schema", "source_schema");
            return new ArrayList<>(row.keySet());
        }

        private Object resultSetValue(Map<String, Object> row, String label) {
            return switch (label) {
                case "name" -> row.get("object_name");
                case "type" -> row.get("object_type");
                case "schema" -> row.get("owner");
                case "source_schema" -> row.get("source_owner");
                default -> row.get(label);
            };
        }

        private Map<String, Object> row(String name, String type, String nullable, int position) {
            Map<String, Object> row = new HashMap<>();
            row.put("column_name", name);
            row.put("data_type", type);
            row.put("nullable", nullable);
            row.put("column_id", position);
            return row;
        }

        private Map<String, Object> objectRow(String name, String type, String owner) {
            return objectRow(name, type, owner, owner);
        }

        private Map<String, Object> objectRow(String name, String type, String owner, String sourceOwner) {
            Map<String, Object> row = new HashMap<>();
            row.put("object_name", name);
            row.put("object_type", type);
            row.put("owner", owner);
            row.put("source_owner", sourceOwner);
            return row;
        }

        private Map<String, Object> synonymRow(String owner, String table) {
            Map<String, Object> row = new HashMap<>();
            row.put("table_owner", owner);
            row.put("table_name", table);
            return row;
        }

        private void setRowsForSql(String sql, List<Map<String, Object>> rows) {
            rowsBySql.put(normalizeSql(sql), rows);
        }

        private String lastSqlNormalized() {
            return normalizeSql(sql);
        }

        private List<String> executedSqlsNormalized() {
            return sqlHistory.stream().map(this::normalizeSql).toList();
        }

        private String normalizeSql(String sql) {
            return sql.stripIndent().trim().replace("\r\n", "\n");
        }
    }
}
