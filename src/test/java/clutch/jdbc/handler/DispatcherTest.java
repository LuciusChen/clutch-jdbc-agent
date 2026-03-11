package clutch.jdbc.handler;

import clutch.jdbc.ConnectionManager;
import clutch.jdbc.CursorManager;
import clutch.jdbc.model.Request;
import clutch.jdbc.model.Response;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

        Response response = dispatcher.dispatch(req);

        assertTrue(response.ok);
        assertEquals("jdbc:test:demo", connMgr.url);
        assertEquals("scott", connMgr.user);
        assertEquals("tiger", connMgr.password);
        assertEquals(Map.of("role", "reporting"), connMgr.props);
        assertEquals(8, connMgr.connectTimeoutSeconds);
        assertEquals(9, connMgr.networkTimeoutSeconds);
        assertEquals(42, ((Number) ((Map<?, ?>) response.result).get("conn-id")).intValue());
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
        private Connection connection;

        @Override
        public int connect(String url, String user, String password, Map<String, String> props,
                           Integer connectTimeoutSeconds, Integer networkTimeoutSeconds) {
            this.url = url;
            this.user = user;
            this.password = password;
            this.props = props;
            this.connectTimeoutSeconds = connectTimeoutSeconds;
            this.networkTimeoutSeconds = networkTimeoutSeconds;
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
}
