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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DispatcherTest {

    @Test
    void connectForwardsExplicitTimeouts() throws Exception {
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        Dispatcher dispatcher = new Dispatcher(connMgr, new CursorManager());
        Request req = new Request();
        req.id = 1;
        req.op = "connect";
        req.params.put("url", "jdbc:test:demo");
        req.params.put("user", "scott");
        req.params.put("password", "tiger");
        req.params.put("props", Map.of("role", "reporting"));
        req.params.put("connect-timeout-seconds", 8);
        req.params.put("network-timeout-seconds", 9);
        req.params.put("auto-commit", false);

        Response response = dispatcher.dispatch(req);

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
        connMgr.connection = (Connection) Proxy.newProxyInstance(
            DispatcherTest.class.getClassLoader(),
            new Class<?>[]{Connection.class},
            (_proxy, method, _args) -> switch (method.getName()) {
                case "commit" -> {
                    committed[0] = true;
                    yield null;
                }
                case "unwrap" -> null;
                case "isWrapperFor" -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });
        Dispatcher dispatcher = new Dispatcher(connMgr, new CursorManager());
        Request req = new Request();
        req.id = 7;
        req.op = "commit";
        req.params.put("conn-id", 7);

        Response response = dispatcher.dispatch(req);

        assertTrue(response.ok);
        assertTrue(committed[0]);
    }

    @Test
    void rollbackCallsConnectionRollback() throws Exception {
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        boolean[] rolledBack = {false};
        connMgr.connection = (Connection) Proxy.newProxyInstance(
            DispatcherTest.class.getClassLoader(),
            new Class<?>[]{Connection.class},
            (_proxy, method, _args) -> switch (method.getName()) {
                case "rollback" -> {
                    rolledBack[0] = true;
                    yield null;
                }
                case "unwrap" -> null;
                case "isWrapperFor" -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });
        Dispatcher dispatcher = new Dispatcher(connMgr, new CursorManager());
        Request req = new Request();
        req.id = 8;
        req.op = "rollback";
        req.params.put("conn-id", 7);

        Response response = dispatcher.dispatch(req);

        assertTrue(response.ok);
        assertTrue(rolledBack[0]);
    }

    @Test
    void setAutoCommitTrueCallsConnectionSetAutoCommit() throws Exception {
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        Boolean[] capturedValue = {null};
        connMgr.connection = (Connection) Proxy.newProxyInstance(
            DispatcherTest.class.getClassLoader(),
            new Class<?>[]{Connection.class},
            (_proxy, method, args) -> switch (method.getName()) {
                case "setAutoCommit" -> {
                    capturedValue[0] = (Boolean) args[0];
                    yield null;
                }
                case "unwrap" -> null;
                case "isWrapperFor" -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });
        Dispatcher dispatcher = new Dispatcher(connMgr, new CursorManager());
        Request req = new Request();
        req.id = 9;
        req.op = "set-auto-commit";
        req.params.put("conn-id", 7);
        req.params.put("auto-commit", true);

        Response response = dispatcher.dispatch(req);

        assertTrue(response.ok);
        assertTrue(capturedValue[0]);
        assertEquals(true, ((Map<?, ?>) response.result).get("auto-commit"));
    }

    @Test
    void setAutoCommitFalseCallsConnectionSetAutoCommit() throws Exception {
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        Boolean[] capturedValue = {null};
        connMgr.connection = (Connection) Proxy.newProxyInstance(
            DispatcherTest.class.getClassLoader(),
            new Class<?>[]{Connection.class},
            (_proxy, method, args) -> switch (method.getName()) {
                case "setAutoCommit" -> {
                    capturedValue[0] = (Boolean) args[0];
                    yield null;
                }
                case "unwrap" -> null;
                case "isWrapperFor" -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });
        Dispatcher dispatcher = new Dispatcher(connMgr, new CursorManager());
        Request req = new Request();
        req.id = 10;
        req.op = "set-auto-commit";
        req.params.put("conn-id", 7);
        req.params.put("auto-commit", false);

        Response response = dispatcher.dispatch(req);

        assertTrue(response.ok);
        assertFalse(capturedValue[0]);
        assertEquals(false, ((Map<?, ?>) response.result).get("auto-commit"));
    }

    @Test
    void executeAppliesQueryTimeoutBeforeRunningStatement() throws Exception {
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        RecordingStatementHandler stmt = new RecordingStatementHandler();
        connMgr.connection = proxyConnection(stmt);
        Dispatcher dispatcher = new Dispatcher(connMgr, new CursorManager());
        Request req = new Request();
        req.id = 2;
        req.op = "execute";
        req.params.put("conn-id", 7);
        req.params.put("sql", "update demo set x = 1;");
        req.params.put("query-timeout-seconds", 16);

        Response response = dispatcher.dispatch(req);

        assertTrue(response.ok);
        assertEquals(16, stmt.queryTimeoutSeconds);
        assertEquals("update demo set x = 1", stmt.executedSql);
        assertTrue(stmt.closed);
        assertEquals("dml", ((Map<?, ?>) response.result).get("type"));
        assertEquals(3, ((Number) ((Map<?, ?>) response.result).get("affected-rows")).intValue());
    }

    @Test
    void executeClosesStatementWhenResultSetFails() throws Exception {
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        boolean[] closed = {false};
        Statement failingStmt = (Statement) Proxy.newProxyInstance(
            DispatcherTest.class.getClassLoader(),
            new Class<?>[]{Statement.class},
            (_proxy, method, args) -> switch (method.getName()) {
                case "execute"      -> true;  // pretend it's a SELECT
                case "getResultSet" -> throw new java.sql.SQLException("simulated rs failure");
                case "close"        -> { closed[0] = true; yield null; }
                case "unwrap"       -> null;
                case "isWrapperFor" -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });
        connMgr.connection = (Connection) Proxy.newProxyInstance(
            DispatcherTest.class.getClassLoader(),
            new Class<?>[]{Connection.class},
            (_proxy, method, _args) -> switch (method.getName()) {
                case "createStatement" -> failingStmt;
                case "unwrap"          -> null;
                case "isWrapperFor"    -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });
        Dispatcher dispatcher = new Dispatcher(connMgr, new CursorManager());
        Request req = new Request();
        req.id = 99;
        req.op = "execute";
        req.params.put("conn-id", 7);
        req.params.put("sql", "SELECT 1");

        assertThrows(java.sql.SQLException.class, () -> dispatcher.dispatch(req));
        assertTrue(closed[0], "statement must be closed when getResultSet() fails");
    }

    @Test
    void getColumnsUsesOracleFastPath() throws Exception {
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        OracleMetadataRecorder oracle = new OracleMetadataRecorder();
        connMgr.connection = oracle.connection();
        Dispatcher dispatcher = new Dispatcher(connMgr, new CursorManager());
        Request req = new Request();
        req.id = 3;
        req.op = "get-columns";
        req.params.put("conn-id", 7);
        req.params.put("schema", "ZJSY");
        req.params.put("table", "t_sys_para");

        Response response = dispatcher.dispatch(req);

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
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> cols = (List<Map<String, Object>>) ((Map<?, ?>) response.result).get("columns");
        assertEquals("PARA_ID", cols.get(0).get("name"));
    }

    @Test
    void searchColumnsUsesOracleFastPathAndPrefix() throws Exception {
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        OracleMetadataRecorder oracle = new OracleMetadataRecorder();
        connMgr.connection = oracle.connection();
        Dispatcher dispatcher = new Dispatcher(connMgr, new CursorManager());
        Request req = new Request();
        req.id = 4;
        req.op = "search-columns";
        req.params.put("conn-id", 7);
        req.params.put("schema", "ZJSY");
        req.params.put("table", "t_sys_para");
        req.params.put("prefix", "pa");

        Response response = dispatcher.dispatch(req);

        assertTrue(response.ok);
        assertEquals(List.of("T_SYS_PARA", "PA%"), oracle.params);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> cols = (List<Map<String, Object>>) ((Map<?, ?>) response.result).get("columns");
        assertEquals(2, cols.size());
        assertEquals("PARA_NAME", cols.get(1).get("name"));
    }

    @Test
    void getPrimaryKeysUsesOracleFastPath() throws Exception {
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        OracleMetadataRecorder oracle = new OracleMetadataRecorder();
        // default resultRows have "column_name" → "PARA_ID" / "PARA_NAME", which the PK query reads
        connMgr.connection = oracle.connection();
        Dispatcher dispatcher = new Dispatcher(connMgr, new CursorManager());
        Request req = new Request();
        req.id = 5;
        req.op = "get-primary-keys";
        req.params.put("conn-id", 7);
        req.params.put("schema", "ZJSY");  // matches currentUser "zjsy" → user_* path
        req.params.put("table", "orders");

        Response response = dispatcher.dispatch(req);

        assertTrue(response.ok);
        assertTrue(oracle.lastSqlNormalized().contains("user_constraints"),
            "should use Oracle fast path (user_constraints)");
        assertEquals(List.of("ORDERS"), oracle.params);
        assertEquals(5, oracle.queryTimeoutSeconds);
        @SuppressWarnings("unchecked")
        List<String> pks = (List<String>) ((Map<?, ?>) response.result).get("primary-keys");
        assertEquals(List.of("PARA_ID", "PARA_NAME"), pks);
    }

    @Test
    void getForeignKeysUsesOracleFastPath() throws Exception {
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        OracleMetadataRecorder oracle = new OracleMetadataRecorder();
        Map<String, Object> fkRow = new HashMap<>();
        fkRow.put("fk_column", "CUSTOMER_ID");
        fkRow.put("pk_table",  "CUSTOMERS");
        fkRow.put("pk_schema", "ZJSY");
        fkRow.put("pk_column", "ID");
        oracle.resultRows = List.of(fkRow);
        connMgr.connection = oracle.connection();
        Dispatcher dispatcher = new Dispatcher(connMgr, new CursorManager());
        Request req = new Request();
        req.id = 6;
        req.op = "get-foreign-keys";
        req.params.put("conn-id", 7);
        req.params.put("schema", "ZJSY");
        req.params.put("table", "orders");

        Response response = dispatcher.dispatch(req);

        assertTrue(response.ok);
        assertTrue(oracle.lastSqlNormalized().contains("user_constraints"),
            "should use Oracle fast path (user_constraints)");
        assertEquals(List.of("ORDERS"), oracle.params);
        assertEquals(5, oracle.queryTimeoutSeconds);
        @SuppressWarnings("unchecked")
        List<Map<?, ?>> fks = (List<Map<?, ?>>) ((Map<?, ?>) response.result).get("foreign-keys");
        assertEquals(1, fks.size());
        assertEquals("CUSTOMER_ID", fks.get(0).get("fk-column"));
        assertEquals("CUSTOMERS",   fks.get(0).get("pk-table"));
        assertEquals("ZJSY",        fks.get(0).get("pk-schema"));
        assertEquals("ID",          fks.get(0).get("pk-column"));
    }

    private static Connection proxyConnection(RecordingStatementHandler stmt) {
        return (Connection) Proxy.newProxyInstance(
            DispatcherTest.class.getClassLoader(),
            new Class<?>[]{Connection.class},
            (_proxy, method, _args) -> switch (method.getName()) {
                case "createStatement" -> stmt.proxy();
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
            assertEquals(7, connId);
            return connection;
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
        private final List<String> params = new ArrayList<>();
        private int queryTimeoutSeconds;
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
            List<Map<String, Object>> rows = this.resultRows;
            return (ResultSet) Proxy.newProxyInstance(
                DispatcherTest.class.getClassLoader(),
                new Class<?>[]{ResultSet.class},
                new java.lang.reflect.InvocationHandler() {
                    int index = -1;
                    @Override
                    public Object invoke(Object _proxy, java.lang.reflect.Method method, Object[] _args) {
                        return switch (method.getName()) {
                            case "next" -> ++index < rows.size();
                            case "getString" -> rows.get(index).get((String) _args[0]);
                            case "getInt" -> rows.get(index).get((String) _args[0]);
                            case "close" -> null;
                            case "unwrap" -> null;
                            case "isWrapperFor" -> false;
                            default -> throw new UnsupportedOperationException(method.getName());
                        };
                    }
                });
        }

        private Map<String, Object> row(String name, String type, String nullable, int position) {
            Map<String, Object> row = new HashMap<>();
            row.put("column_name", name);
            row.put("data_type", type);
            row.put("nullable", nullable);
            row.put("column_id", position);
            return row;
        }

        private String lastSqlNormalized() {
            return sql.stripIndent().trim().replace("\r\n", "\n");
        }
    }
}
