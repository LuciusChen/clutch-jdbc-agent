package clutch.jdbc.handler;

import clutch.jdbc.ConnectionManager;
import clutch.jdbc.CursorManager;
import clutch.jdbc.model.Request;
import clutch.jdbc.model.Response;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.DatabaseMetaData;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DispatcherTest {

    @ParameterizedTest
    @MethodSource("connectCases")
    void connectForwardsExplicitTimeouts(String url, String user, String password,
                                         Map<String, String> props,
                                         int connectTimeoutSeconds,
                                         int networkTimeoutSeconds,
                                         boolean autoCommit,
                                         int returnedConnId) throws Exception {
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        connMgr.returnedConnId = returnedConnId;
        Response response = dispatch(connMgr, 1, "connect",
            "url", url,
            "user", user,
            "password", password,
            "props", props,
            "connect-timeout-seconds", connectTimeoutSeconds,
            "network-timeout-seconds", networkTimeoutSeconds,
            "auto-commit", autoCommit);

        assertTrue(response.ok);
        assertEquals(url, connMgr.url);
        assertEquals(user, connMgr.user);
        assertEquals(password, connMgr.password);
        assertEquals(props, connMgr.props);
        assertEquals(connectTimeoutSeconds, connMgr.connectTimeoutSeconds);
        assertEquals(networkTimeoutSeconds, connMgr.networkTimeoutSeconds);
        assertEquals(autoCommit, connMgr.autoCommit);
        assertEquals(returnedConnId, ((Number) ((Map<?, ?>) response.result).get("conn-id")).intValue());
    }

    @Test
    void connectReturnsDiagnosticErrorWhenConnectionFails() throws Exception {
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        connMgr.connectFailure = new java.sql.SQLNonTransientConnectionException(
            "reason-61",
            "08061",
            new java.net.ConnectException("root-61"));

        Response response = dispatch(connMgr, 61, "connect",
            "url", "jdbc:clickhouse://127.0.0.1:8123/testdb?password=secret-61&ssl=true",
            "user", "test",
            "password", "test",
            "props", Map.of("http_header_COOKIE", "cookie-secret-61", "socket_timeout", "10"));

        assertFalse(response.ok);
        assertNotNull(response.error);
        assertTrue(response.error.contains("SQLNonTransientConnectionException"));
        assertTrue(response.error.contains("08061"));
        assertTrue(response.error.contains("reason-61"));
        assertTrue(response.error.contains("ConnectException"));
        assertTrue(response.error.contains("root-61"));
        Object diagObject = response.getClass().getField("diag").get(response);
        assertTrue(diagObject instanceof Map<?, ?>);
        Map<?, ?> diag = (Map<?, ?>) diagObject;
        assertEquals("connect", diag.get("category"));
        assertEquals("connect", diag.get("op"));
        assertEquals(61, ((Number) diag.get("request-id")).intValue());
        assertEquals("java.sql.SQLNonTransientConnectionException", diag.get("exception-class"));
        assertEquals("08061", diag.get("sql-state"));
        assertTrue(diag.get("cause-chain") instanceof List<?>);
        @SuppressWarnings("unchecked")
        Map<String, Object> context = (Map<String, Object>) diag.get("context");
        assertEquals("test", context.get("user"));
        String redactedUrl61 = (String) context.get("redacted-url");
        assertTrue(redactedUrl61.contains("?password=<redacted>"),
            "redacted URL must preserve '?' delimiter");
        assertFalse(redactedUrl61.contains("secret-61"));
        assertTrue(redactedUrl61.contains("ssl=true"),
            "non-secret URL parameter 'ssl=true' must be preserved");
        assertFalse(context.containsKey("password"));
        assertEquals(List.of("http_header_COOKIE", "socket_timeout"), context.get("property-keys"));
        assertFalse(diag.toString().contains("cookie-secret-61"));
    }

    @Test
    void connectDoesNotReturnDebugPayloadByDefault() throws Exception {
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        connMgr.connectFailure = new java.sql.SQLNonTransientConnectionException(
            "reason-61",
            "08061",
            new java.net.ConnectException("root-61"));

        Response response = dispatch(connMgr, 76, "connect",
            "url", "jdbc:clickhouse://127.0.0.1:8123/testdb?password=secret-76&ssl=true",
            "user", "test",
            "password", "test",
            "props", Map.of("http_header_COOKIE", "cookie-secret-76", "socket_timeout", "10"));

        assertFalse(response.ok);
        assertNull(response.getClass().getField("debug").get(response));
    }

    @Test
    void connectDebugPayloadIsOptInAndRedacted() throws Exception {
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        connMgr.connectFailure = new java.sql.SQLNonTransientConnectionException(
            "reason-77 http_header_COOKIE=abc123",
            "08061",
            new java.net.ConnectException("root-77 secret-77"));

        Response response = dispatch(connMgr, 77, "connect",
            "url", "jdbc:clickhouse://127.0.0.1:8123/testdb?password=secret-77&ssl=true",
            "user", "test",
            "password", "test",
            "props", Map.of("http_header_COOKIE", "abc123", "socket_timeout", "10"),
            "debug", true);

        assertFalse(response.ok);
        @SuppressWarnings("unchecked")
        Map<String, Object> debug = (Map<String, Object>) response.getClass().getField("debug").get(response);
        assertNotNull(debug);
        assertTrue(debug.get("thread") instanceof String);
        assertFalse(((String) debug.get("thread")).isBlank());
        @SuppressWarnings("unchecked")
        Map<String, Object> requestContext = (Map<String, Object>) debug.get("request-context");
        assertEquals("test", requestContext.get("user"));
        assertTrue(Objects.toString(requestContext.get("redacted-url"), "").contains("<redacted>"));
        String stackTrace = Objects.toString(debug.get("stack-trace"), "");
        assertTrue(stackTrace.contains("SQLNonTransientConnectionException"));
        assertFalse(stackTrace.contains("secret-77"));
        assertFalse(stackTrace.contains("abc123"));
    }

    @Test
    void connectDiagnosticsRedactSemicolonStyleUrlAndSecretMessages() throws Exception {
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        String url =
            "jdbc:sqlserver://db.local:1433;encrypt=true;user=sa;password=secret-62;accessToken=token-62";
        connMgr.connectFailure = new java.sql.SQLNonTransientConnectionException(
            "connect failed for " + url + " cookie cookie-secret-62",
            "08001",
            new SQLException("root cause " + url + " cookie cookie-secret-62"));

        Response response = dispatch(connMgr, 62, "connect",
            "url", url,
            "user", "sa",
            "password", "password-param-62",
            "props", Map.of("http_header_COOKIE", "cookie-secret-62", "socket_timeout", "10"));

        assertFalse(response.ok);
        assertFalse(response.error.contains("secret-62"));
        assertFalse(response.error.contains("token-62"));
        assertFalse(response.error.contains("cookie-secret-62"));
        Map<String, Object> diag = diagMap(response);
        @SuppressWarnings("unchecked")
        Map<String, Object> context = (Map<String, Object>) diag.get("context");
        String redactedUrl = (String) context.get("redacted-url");
        assertTrue(redactedUrl.startsWith("jdbc:sqlserver://db.local:1433;"),
            "redacted URL must preserve ';' delimiter after host");
        assertTrue(redactedUrl.contains(";password=<redacted>"));
        assertTrue(redactedUrl.contains(";accessToken=<redacted>"));
        assertTrue(redactedUrl.contains("encrypt=true"),
            "non-secret URL parameter 'encrypt=true' must be preserved");
        assertTrue(redactedUrl.contains("user=sa"),
            "non-secret URL parameter 'user=sa' must be preserved");
        assertFalse(redactedUrl.contains("secret-62"));
        assertFalse(redactedUrl.contains("token-62"));
        assertFalse(Objects.toString(diag.get("raw-message"), "").contains("secret-62"));
        assertFalse(Objects.toString(diag.get("raw-message"), "").contains("token-62"));
        assertFalse(Objects.toString(diag.get("raw-message"), "").contains("cookie-secret-62"));
        assertFalse(Objects.toString(diag.get("cause-chain"), "").contains("secret-62"));
        assertFalse(Objects.toString(diag.get("cause-chain"), "").contains("token-62"));
        assertFalse(Objects.toString(diag.get("cause-chain"), "").contains("cookie-secret-62"));
    }

    @Test
    void shortPasswordDoesNotOverRedactDiagnosticText() throws Exception {
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        // Exception message deliberately contains the word "test" in diagnostic context
        connMgr.connectFailure = new java.sql.SQLNonTransientConnectionException(
            "test connection refused by testdb host",
            "08001",
            new java.net.ConnectException("Connection to test-server timed out"));

        Response response = dispatch(connMgr, 70, "connect",
            "url", "jdbc:postgresql://localhost:5432/testdb?password=secret-70&sslmode=require",
            "user", "testuser",
            "password", "test",
            "props", Map.of());

        assertFalse(response.ok);
        // The word "test" must NOT be redacted in diagnostic text
        assertTrue(response.error.contains("test connection refused"),
            "diagnostic text 'test connection refused' should not be over-redacted");
        assertTrue(response.error.contains("testdb"),
            "database name 'testdb' should not be over-redacted");
        assertTrue(response.error.contains("test-server"),
            "hostname 'test-server' should not be over-redacted");
        // But the URL password must still be redacted
        Map<String, Object> diag70 = diagMap(response);
        @SuppressWarnings("unchecked")
        Map<String, Object> ctx70 = (Map<String, Object>) diag70.get("context");
        String redactedUrl70 = (String) ctx70.get("redacted-url");
        assertTrue(redactedUrl70.contains("?password=<redacted>"),
            "redacted URL must preserve '?' delimiter");
        assertFalse(redactedUrl70.contains("secret-70"));
        assertTrue(redactedUrl70.contains("sslmode=require"),
            "non-secret URL parameter 'sslmode=require' must be preserved");
    }

    @Test
    void urlSecretsStillRedactedWithNewSanitization() throws Exception {
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        String url = "jdbc:mysql://db:3306/app?password=secret-71&token=tok-71-value&ssl=true";
        connMgr.connectFailure = new java.sql.SQLNonTransientConnectionException(
            "Access denied: " + url,
            "28000");

        Response response = dispatch(connMgr, 71, "connect",
            "url", url,
            "user", "admin",
            "password", "admin-pass-71",
            "props", Map.of());

        assertFalse(response.ok);
        assertFalse(response.error.contains("secret-71"),
            "URL password value must be redacted from error text");
        assertFalse(response.error.contains("tok-71-value"),
            "URL token value must be redacted from error text");
        Map<String, Object> diag71 = diagMap(response);
        assertFalse(Objects.toString(diag71.get("raw-message"), "").contains("secret-71"));
        assertFalse(Objects.toString(diag71.get("raw-message"), "").contains("tok-71-value"));
    }

    @Test
    void cookieValuesRedactedWithNewSanitization() throws Exception {
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        connMgr.connectFailure = new java.sql.SQLNonTransientConnectionException(
            "auth failed cookie session-cookie-72 token: header-token-72",
            "28000",
            new SQLException("root cookie session-cookie-72"));

        Response response = dispatch(connMgr, 72, "connect",
            "url", "jdbc:clickhouse://127.0.0.1:8123/db",
            "user", "user",
            "password", "pw-72-longenough",
            "props", Map.of("http_header_COOKIE", "session-cookie-72",
                            "http_header_Authorization", "header-token-72",
                            "socket_timeout", "10"));

        assertFalse(response.ok);
        assertFalse(response.error.contains("session-cookie-72"),
            "cookie value must be redacted from error text");
        assertFalse(response.error.contains("header-token-72"),
            "authorization header value must be redacted from error text");
        Map<String, Object> diag72 = diagMap(response);
        assertFalse(Objects.toString(diag72.get("cause-chain"), "").contains("session-cookie-72"));
    }

    @Test
    void nonSensitiveMetadataNotRedacted() throws Exception {
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        // Exception references schema/table names that happen to contain secret-like substrings
        connMgr.connectFailure = new java.sql.SQLNonTransientConnectionException(
            "Cannot find catalog 'tokenizer_db' schema 'auth_service' table 'credentials_log'",
            "42000");

        Response response = dispatch(connMgr, 73, "connect",
            "url", "jdbc:postgresql://localhost/tokenizer_db",
            "user", "app",
            "password", "real-secret-73-pwd",
            "props", Map.of());

        assertFalse(response.ok);
        // These identifiers contain substrings of secret key names but are NOT secrets
        assertTrue(response.error.contains("tokenizer_db"),
            "catalog name 'tokenizer_db' must not be redacted");
        assertTrue(response.error.contains("auth_service"),
            "schema name 'auth_service' must not be redacted");
        assertTrue(response.error.contains("credentials_log"),
            "table name 'credentials_log' must not be redacted");
    }

    @Test
    void longSecretInsideSnakeOrKebabIdentifierNotOverRedacted() throws Exception {
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        // Password is a long value that also appears as a prefix in identifiers
        connMgr.connectFailure = new java.sql.SQLNonTransientConnectionException(
            "Cannot find table 'warehouse01_orders' on host warehouse01-db.local",
            "42000");

        Response response = dispatch(connMgr, 74, "connect",
            "url", "jdbc:postgresql://warehouse01-db.local/app",
            "user", "app",
            "password", "warehouse01",
            "props", Map.of());

        assertFalse(response.ok);
        assertTrue(response.error.contains("warehouse01_orders"),
            "snake_case identifier containing the password must not be over-redacted");
        assertTrue(response.error.contains("warehouse01-db"),
            "kebab-case hostname containing the password must not be over-redacted");
    }

    @Test
    void shortPrefixedSecretKeysStillRedactValues() throws Exception {
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        connMgr.connectFailure = new java.sql.SQLNonTransientConnectionException(
            "connect failed http_header_COOKIE=abc123 proxyAuthorization: xyz789",
            "28000",
            new SQLException("root http_header_COOKIE=abc123 proxyAuthorization: xyz789"));

        Response response = dispatch(connMgr, 75, "connect",
            "url", "jdbc:clickhouse://127.0.0.1:8123/db",
            "user", "user",
            "password", "pw-75-longenough",
            "props", Map.of("http_header_COOKIE", "abc123",
                            "proxyAuthorization", "xyz789",
                            "socket_timeout", "10"));

        assertFalse(response.ok);
        assertFalse(response.error.contains("abc123"),
            "short cookie value must be redacted from error text");
        assertFalse(response.error.contains("xyz789"),
            "short authorization value must be redacted from error text");
        Map<String, Object> diag75 = diagMap(response);
        assertFalse(Objects.toString(diag75.get("raw-message"), "").contains("abc123"));
        assertFalse(Objects.toString(diag75.get("raw-message"), "").contains("xyz789"));
        assertFalse(Objects.toString(diag75.get("cause-chain"), "").contains("abc123"));
        assertFalse(Objects.toString(diag75.get("cause-chain"), "").contains("xyz789"));
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
    void setCurrentSchemaFailureCarriesGeneratedSql() throws Exception {
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        String token = "MISSING_SCHEMA_13";
        connMgr.connection = failingOracleSchemaConnection(token);
        Response response = dispatch(connMgr, 13, "set-current-schema",
            "conn-id", 7,
            "schema", token);

        assertFalse(response.ok);
        Object diagObject = response.getClass().getField("diag").get(response);
        assertTrue(diagObject instanceof Map<?, ?>);
        Map<?, ?> diag = (Map<?, ?>) diagObject;
        assertEquals("metadata", diag.get("category"));
        assertEquals("set-current-schema", diag.get("op"));
        assertEquals(13, ((Number) diag.get("request-id")).intValue());
        @SuppressWarnings("unchecked")
        Map<String, Object> context = (Map<String, Object>) diag.get("context");
        assertEquals(token, context.get("schema"));
        assertTrue(context.get("generated-sql") instanceof String);
        assertTrue(((String) context.get("generated-sql")).contains("ALTER SESSION SET CURRENT_SCHEMA"));
        assertTrue(((String) context.get("generated-sql")).contains(token));
        assertTrue(Objects.toString(diag.get("raw-message"), "").contains(token));
    }

    @ParameterizedTest
    @MethodSource("executeDmlCases")
    void executeAppliesQueryTimeoutBeforeRunningStatement(String sql,
                                                          int queryTimeoutSeconds,
                                                          int updateCount,
                                                          String expectedSql) throws Exception {
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        RecordingStatementHandler stmt = new RecordingStatementHandler();
        stmt.updateCount = updateCount;
        connMgr.connection = proxyConnection(stmt);
        Response response = dispatch(connMgr, 2, "execute",
            "conn-id", 7,
            "sql", sql,
            "query-timeout-seconds", queryTimeoutSeconds);

        assertTrue(response.ok);
        assertEquals(queryTimeoutSeconds, stmt.queryTimeoutSeconds);
        assertEquals(expectedSql, stmt.executedSql);
        assertTrue(stmt.closed);
        assertEquals("dml", resultMap(response).get("type"));
        assertEquals(updateCount, ((Number) resultMap(response).get("affected-rows")).intValue());
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
        Response response = dispatch(connMgr, 99, "execute", "conn-id", 7, "sql", "SELECT 1");
        assertFalse(response.ok);
        assertEquals("simulated rs failure", response.error);
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
        Object diagObject = response.getClass().getField("diag").get(response);
        assertTrue(diagObject instanceof Map<?, ?>);
        Map<?, ?> diag = (Map<?, ?>) diagObject;
        assertEquals("connection-lost", diag.get("category"));
        assertEquals("execute", diag.get("op"));
        assertEquals(19, ((Number) diag.get("request-id")).intValue());
        assertEquals(7, ((Number) diag.get("conn-id")).intValue());
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
    void cancelInterruptsRunningExecuteAndKeepsConnectionUsable() throws Exception {
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        CountDownLatch executeStarted = new CountDownLatch(1);
        CountDownLatch cancelCalled = new CountDownLatch(1);
        List<String> executedSql = new ArrayList<>();

        Statement blockingStmt = (Statement) Proxy.newProxyInstance(
            DispatcherTest.class.getClassLoader(),
            new Class<?>[]{Statement.class},
            (_proxy, method, args) -> switch (method.getName()) {
                case "setQueryTimeout" -> null;
                case "execute" -> {
                    executedSql.add((String) args[0]);
                    executeStarted.countDown();
                    if (!cancelCalled.await(2, TimeUnit.SECONDS)) {
                        throw new AssertionError("cancel was not requested");
                    }
                    throw new java.sql.SQLException("Query cancelled");
                }
                case "cancel" -> {
                    cancelCalled.countDown();
                    yield null;
                }
                case "close" -> null;
                case "unwrap" -> null;
                case "isWrapperFor" -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });

        RecordingStatementHandler followupStmt = new RecordingStatementHandler();
        followupStmt.updateCount = 1;
        Statement[] statements = {blockingStmt, followupStmt.proxy()};
        int[] nextStatement = {0};
        connMgr.connection = (Connection) Proxy.newProxyInstance(
            DispatcherTest.class.getClassLoader(),
            new Class<?>[]{Connection.class},
            (_proxy, method, _args) -> switch (method.getName()) {
                case "isValid" -> true;
                case "createStatement" -> statements[nextStatement[0]++];
                case "unwrap" -> null;
                case "isWrapperFor" -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });

        Dispatcher dispatcher = new Dispatcher(connMgr, new CursorManager());
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<Response> executeFuture = pool.submit(
                () -> dispatcher.dispatch(request(40, "execute", "conn-id", 7, "sql", "SELECT * FROM t")));

            assertTrue(executeStarted.await(2, TimeUnit.SECONDS), "execute should start before cancel");

            Response cancelResponse = dispatcher.dispatch(request(41, "cancel", "conn-id", 7));
            assertTrue(cancelResponse.ok);
            assertEquals(true, resultMap(cancelResponse).get("cancelled"));

            Response interrupted = executeFuture.get(2, TimeUnit.SECONDS);
            assertFalse(interrupted.ok);
            assertEquals("Query cancelled", interrupted.error);

            Response followup = dispatcher.dispatch(
                request(42, "execute", "conn-id", 7, "sql", "UPDATE demo SET x = 1"));
            assertTrue(followup.ok);
            assertEquals("dml", resultMap(followup).get("type"));
            assertEquals(1, ((Number) resultMap(followup).get("affected-rows")).intValue());
            assertEquals(List.of("SELECT * FROM t"), executedSql);
            assertEquals("UPDATE demo SET x = 1", followupStmt.executedSql);
        } finally {
            pool.shutdownNow();
            dispatcher.shutdown();
        }
    }

    @Test
    void cancelInterruptsRunningFetchAndKeepsConnectionUsable() throws Exception {
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        CountDownLatch fetchStarted = new CountDownLatch(1);
        CountDownLatch cancelCalled = new CountDownLatch(1);

        Statement blockingStmt = (Statement) Proxy.newProxyInstance(
            DispatcherTest.class.getClassLoader(),
            new Class<?>[]{Statement.class},
            (_proxy, method, _args) -> switch (method.getName()) {
                case "cancel" -> {
                    cancelCalled.countDown();
                    yield null;
                }
                case "close" -> null;
                case "unwrap" -> null;
                case "isWrapperFor" -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });

        ResultSetMetaData meta = (ResultSetMetaData) Proxy.newProxyInstance(
            DispatcherTest.class.getClassLoader(),
            new Class<?>[]{ResultSetMetaData.class},
            (_proxy, method, args) -> switch (method.getName()) {
                case "getColumnCount" -> 1;
                case "getColumnLabel" -> "id";
                case "getColumnTypeName" -> "INTEGER";
                case "unwrap" -> null;
                case "isWrapperFor" -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });

        ResultSet blockingRs = (ResultSet) Proxy.newProxyInstance(
            DispatcherTest.class.getClassLoader(),
            new Class<?>[]{ResultSet.class},
            (_proxy, method, _args) -> switch (method.getName()) {
                case "getMetaData" -> meta;
                case "next" -> {
                    fetchStarted.countDown();
                    if (!cancelCalled.await(2, TimeUnit.SECONDS)) {
                        throw new AssertionError("cancel was not requested");
                    }
                    throw new java.sql.SQLException("Query cancelled");
                }
                case "close" -> null;
                case "unwrap" -> null;
                case "isWrapperFor" -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });

        RecordingStatementHandler followupStmt = new RecordingStatementHandler();
        followupStmt.updateCount = 1;
        connMgr.connection = (Connection) Proxy.newProxyInstance(
            DispatcherTest.class.getClassLoader(),
            new Class<?>[]{Connection.class},
            (_proxy, method, _args) -> switch (method.getName()) {
                case "isValid" -> true;
                case "createStatement" -> followupStmt.proxy();
                case "unwrap" -> null;
                case "isWrapperFor" -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });

        CursorManager cursorMgr = new CursorManager();
        int cursorId = cursorMgr.register(7, blockingStmt, blockingRs);
        Dispatcher dispatcher = new Dispatcher(connMgr, cursorMgr);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<Response> fetchFuture = pool.submit(
                () -> dispatcher.dispatch(request(43, "fetch", "cursor-id", cursorId, "fetch-size", 10)));

            assertTrue(fetchStarted.await(2, TimeUnit.SECONDS), "fetch should start before cancel");

            Response cancelResponse = dispatcher.dispatch(request(44, "cancel", "conn-id", 7));
            assertTrue(cancelResponse.ok);
            assertEquals(true, resultMap(cancelResponse).get("cancelled"));

            Response interrupted = fetchFuture.get(2, TimeUnit.SECONDS);
            assertFalse(interrupted.ok);
            assertEquals("Query cancelled", interrupted.error);
            assertEquals("cancel", diagMap(interrupted).get("category"));

            Response followup = dispatcher.dispatch(
                request(45, "execute", "conn-id", 7, "sql", "UPDATE demo SET x = 1"));
            assertTrue(followup.ok);
            assertEquals("dml", resultMap(followup).get("type"));
            assertEquals(1, ((Number) resultMap(followup).get("affected-rows")).intValue());
            assertEquals("UPDATE demo SET x = 1", followupStmt.executedSql);
        } finally {
            pool.shutdownNow();
            dispatcher.shutdown();
        }
    }

    @Test
    void fetchTimesOutAndCancelsStatementWhenBlocked() throws Exception {
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        boolean[] cancelled = {false};
        boolean[] statementClosed = {false};
        boolean[] resultSetClosed = {false};

        Statement blockingStmt = (Statement) Proxy.newProxyInstance(
            DispatcherTest.class.getClassLoader(),
            new Class<?>[]{Statement.class},
            (_proxy, method, _args) -> switch (method.getName()) {
                case "cancel" -> {
                    cancelled[0] = true;
                    yield null;
                }
                case "close" -> {
                    statementClosed[0] = true;
                    yield null;
                }
                case "unwrap" -> null;
                case "isWrapperFor" -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });

        ResultSetMetaData meta = (ResultSetMetaData) Proxy.newProxyInstance(
            DispatcherTest.class.getClassLoader(),
            new Class<?>[]{ResultSetMetaData.class},
            (_proxy, method, args) -> switch (method.getName()) {
                case "getColumnCount" -> 1;
                case "getColumnLabel" -> "id";
                case "getColumnTypeName" -> "INTEGER";
                case "unwrap" -> null;
                case "isWrapperFor" -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });

        ResultSet blockingRs = (ResultSet) Proxy.newProxyInstance(
            DispatcherTest.class.getClassLoader(),
            new Class<?>[]{ResultSet.class},
            (_proxy, method, _args) -> switch (method.getName()) {
                case "getMetaData" -> meta;
                case "next" -> {
                    try {
                        Thread.sleep(Long.MAX_VALUE);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    throw new java.sql.SQLException("interrupted");
                }
                case "close" -> {
                    resultSetClosed[0] = true;
                    yield null;
                }
                case "unwrap" -> null;
                case "isWrapperFor" -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });

        RecordingStatementHandler followupStmt = new RecordingStatementHandler();
        followupStmt.updateCount = 1;
        connMgr.connection = (Connection) Proxy.newProxyInstance(
            DispatcherTest.class.getClassLoader(),
            new Class<?>[]{Connection.class},
            (_proxy, method, _args) -> switch (method.getName()) {
                case "isValid" -> true;
                case "createStatement" -> followupStmt.proxy();
                case "unwrap" -> null;
                case "isWrapperFor" -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });

        CursorManager cursorMgr = new CursorManager();
        int cursorId = cursorMgr.register(7, blockingStmt, blockingRs);
        Dispatcher dispatcher = new Dispatcher(connMgr, cursorMgr);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            long start = System.currentTimeMillis();
            Future<Response> fetchFuture = pool.submit(
                () -> dispatcher.dispatch(request(46, "fetch",
                    "cursor-id", cursorId,
                    "fetch-size", 10,
                    "query-timeout-seconds", 1)));

            Response timedOut = fetchFuture.get(4, TimeUnit.SECONDS);
            long elapsed = System.currentTimeMillis() - start;

            assertFalse(timedOut.ok);
            assertNotNull(timedOut.error);
            assertTrue(timedOut.error.contains("timed out"),
                "error should mention timed out: " + timedOut.error);
            assertEquals("timeout", diagMap(timedOut).get("category"));
            assertTrue(cancelled[0], "stmt.cancel() must be called on fetch timeout");
            assertTrue(statementClosed[0], "stmt.close() must be called on fetch timeout");
            assertTrue(resultSetClosed[0], "resultset must be closed on fetch timeout");
            assertTrue(elapsed < 4000, "test must complete within 4s, took: " + elapsed + "ms");

            Response followup = dispatcher.dispatch(
                request(47, "execute", "conn-id", 7, "sql", "UPDATE demo SET x = 1"));
            assertTrue(followup.ok);
            assertEquals("dml", resultMap(followup).get("type"));
            assertEquals(1, ((Number) resultMap(followup).get("affected-rows")).intValue());
            assertEquals("UPDATE demo SET x = 1", followupStmt.executedSql);
        } finally {
            pool.shutdownNow();
            dispatcher.shutdown();
        }
    }

    @Test
    void executeTimesOutWhenInitialFetchBlocksAndKeepsConnectionUsable() throws Exception {
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        boolean[] cancelled = {false};
        boolean[] statementClosed = {false};
        boolean[] resultSetClosed = {false};

        ResultSetMetaData meta = (ResultSetMetaData) Proxy.newProxyInstance(
            DispatcherTest.class.getClassLoader(),
            new Class<?>[]{ResultSetMetaData.class},
            (_proxy, method, args) -> switch (method.getName()) {
                case "getColumnCount" -> 1;
                case "getColumnLabel" -> "id";
                case "getColumnTypeName" -> "INTEGER";
                case "unwrap" -> null;
                case "isWrapperFor" -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });

        ResultSet blockingRs = (ResultSet) Proxy.newProxyInstance(
            DispatcherTest.class.getClassLoader(),
            new Class<?>[]{ResultSet.class},
            (_proxy, method, _args) -> switch (method.getName()) {
                case "getMetaData" -> meta;
                case "setFetchSize" -> null;
                case "next" -> {
                    try {
                        Thread.sleep(Long.MAX_VALUE);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    throw new java.sql.SQLException("interrupted");
                }
                case "close" -> {
                    resultSetClosed[0] = true;
                    yield null;
                }
                case "unwrap" -> null;
                case "isWrapperFor" -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });

        Statement[] createdStatement = new Statement[1];
        Statement queryStmt = (Statement) Proxy.newProxyInstance(
            DispatcherTest.class.getClassLoader(),
            new Class<?>[]{Statement.class},
            (_proxy, method, args) -> switch (method.getName()) {
                case "setQueryTimeout" -> null;
                case "execute" -> true;
                case "getResultSet" -> blockingRs;
                case "cancel" -> {
                    cancelled[0] = true;
                    yield null;
                }
                case "close" -> {
                    statementClosed[0] = true;
                    yield null;
                }
                case "unwrap" -> null;
                case "isWrapperFor" -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });
        createdStatement[0] = queryStmt;

        RecordingStatementHandler followupStmt = new RecordingStatementHandler();
        followupStmt.updateCount = 1;
        connMgr.connection = (Connection) Proxy.newProxyInstance(
            DispatcherTest.class.getClassLoader(),
            new Class<?>[]{Connection.class},
            (_proxy, method, _args) -> switch (method.getName()) {
                case "isValid" -> true;
                case "createStatement" -> {
                    Statement next = createdStatement[0];
                    createdStatement[0] = followupStmt.proxy();
                    yield next;
                }
                case "unwrap" -> null;
                case "isWrapperFor" -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });

        Dispatcher dispatcher = new Dispatcher(connMgr, new CursorManager());
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            long start = System.currentTimeMillis();
            Future<Response> executeFuture = pool.submit(
                () -> dispatcher.dispatch(request(48, "execute",
                    "conn-id", 7,
                    "sql", "SELECT * FROM t",
                    "fetch-size", 10,
                    "query-timeout-seconds", 1)));

            Response timedOut = executeFuture.get(4, TimeUnit.SECONDS);
            long elapsed = System.currentTimeMillis() - start;

            assertFalse(timedOut.ok);
            assertNotNull(timedOut.error);
            assertTrue(timedOut.error.contains("timed out"),
                "error should mention timed out: " + timedOut.error);
            assertEquals("timeout", diagMap(timedOut).get("category"));
            assertTrue(cancelled[0], "stmt.cancel() must be called when initial fetch times out");
            assertTrue(statementClosed[0], "stmt.close() must be called when initial fetch times out");
            assertTrue(resultSetClosed[0], "resultset must be closed when initial fetch times out");
            assertTrue(elapsed < 4000, "test must complete within 4s, took: " + elapsed + "ms");

            Response followup = dispatcher.dispatch(
                request(49, "execute", "conn-id", 7, "sql", "UPDATE demo SET x = 1"));
            assertTrue(followup.ok);
            assertEquals("dml", resultMap(followup).get("type"));
            assertEquals(1, ((Number) resultMap(followup).get("affected-rows")).intValue());
            assertEquals("UPDATE demo SET x = 1", followupStmt.executedSql);
        } finally {
            pool.shutdownNow();
            dispatcher.shutdown();
        }
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
    void getTablesForGenericMetadataUsesCatalogWhenProvided() throws Exception {
        JdbcMetadataRecorder jdbc = new JdbcMetadataRecorder();
        jdbc.tableRows = List.of(
            jdbc.tableRow("clutch_live_smoke", "TABLE", "default", ""),
            jdbc.tableRow("clutch_live_smoke", "TABLE", "demo", "")
        );
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        connMgr.connection = jdbc.connection("ClickHouse");

        Response response = dispatch(connMgr, 34, "get-tables",
            "conn-id", 7,
            "catalog", "default");

        assertTrue(response.ok);
        assertEquals("getTables", jdbc.lastMethod);
        assertEquals("default", jdbc.lastCatalog);
        assertEquals(null, jdbc.lastSchema);
        List<List<Object>> rows = resultList(response, "rows");
        assertEquals(1, rows.size());
        assertEquals(List.of("clutch_live_smoke", "TABLE", "default"), rows.get(0));
    }

    @Test
    void searchTablesForGenericMetadataUsesCatalogWhenProvided() throws Exception {
        JdbcMetadataRecorder jdbc = new JdbcMetadataRecorder();
        jdbc.tableRows = List.of(
            jdbc.tableRow("clutch_live_smoke", "TABLE", "default", ""),
            jdbc.tableRow("clutch_live_smoke", "TABLE", "demo", "")
        );
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        connMgr.connection = jdbc.connection("ClickHouse");

        Response response = dispatch(connMgr, 35, "search-tables",
            "conn-id", 7,
            "catalog", "default",
            "prefix", "clutch");

        assertTrue(response.ok);
        assertEquals("getTables", jdbc.lastMethod);
        assertEquals("default", jdbc.lastCatalog);
        assertEquals(null, jdbc.lastSchema);
        assertEquals("clutch%", jdbc.lastPattern);
        List<Map<String, Object>> tables = resultList(response, "tables");
        assertEquals(1, tables.size());
        assertEquals("clutch_live_smoke", tables.get(0).get("name"));
        assertEquals("default", tables.get(0).get("schema"));
    }

    @Test
    void getColumnsForGenericMetadataUsesCatalogWhenProvided() throws Exception {
        JdbcMetadataRecorder jdbc = new JdbcMetadataRecorder();
        jdbc.columnRows = List.of(
            jdbc.columnRow("id", "UInt64", 0, 1, "", "default"),
            jdbc.columnRow("demo_only", "UInt64", 0, 1, "", "demo"),
            jdbc.columnRow("name", "String", DatabaseMetaData.columnNullable, 2, "", "default")
        );
        RecordingConnectionManager connMgr = new RecordingConnectionManager();
        connMgr.connection = jdbc.connection("ClickHouse");

        Response response = dispatch(connMgr, 36, "get-columns",
            "conn-id", 7,
            "catalog", "default",
            "table", "clutch_live_smoke");

        assertTrue(response.ok);
        assertEquals("getColumns", jdbc.lastMethod);
        assertEquals("default", jdbc.lastCatalog);
        assertEquals(null, jdbc.lastSchema);
        assertEquals("clutch_live_smoke", jdbc.lastTable);
        List<Map<String, Object>> cols = resultList(response, "columns");
        assertEquals(2, cols.size());
        assertEquals("id", cols.get(0).get("name"));
        assertEquals(false, cols.get(0).get("nullable"));
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

    private static Stream<Arguments> connectCases() {
        return Stream.of(
            Arguments.of("jdbc:test:demo", "scott", "tiger",
                Map.of("role", "reporting"), 8, 9, false, 417),
            Arguments.of("jdbc:test:analytics", "app_reader", "s3cr3t!",
                Map.of("schema", "warehouse", "ssl", "true"), 3, 27, true, 9021)
        );
    }

    private static Stream<Arguments> executeDmlCases() {
        return Stream.of(
            Arguments.of("update demo set x = 1;", 16, 17, "update demo set x = 1"),
            Arguments.of("DELETE FROM audit_log WHERE id = 99;;", 5, 2, "DELETE FROM audit_log WHERE id = 99")
        );
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
    private static Map<String, Object> diagMap(Response response) throws Exception {
        return (Map<String, Object>) response.getClass().getField("diag").get(response);
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

    private static Connection failingOracleSchemaConnection(String token) {
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
                case "execute" -> throw new SQLException("schema-switch-" + token);
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
        private int returnedConnId = 42;
        private String url;
        private String user;
        private String password;
        private Map<String, String> props;
        private Integer connectTimeoutSeconds;
        private Integer networkTimeoutSeconds;
        private boolean autoCommit = true;
        private Connection connection;
        private Connection metadataConnection;
        private SQLException connectFailure;

        @Override
        public int connect(String url, String user, String password, Map<String, String> props,
                           Integer connectTimeoutSeconds, Integer networkTimeoutSeconds,
                           boolean autoCommit) throws SQLException {
            this.url = url;
            this.user = user;
            this.password = password;
            this.props = props;
            this.connectTimeoutSeconds = connectTimeoutSeconds;
            this.networkTimeoutSeconds = networkTimeoutSeconds;
            this.autoCommit = autoCommit;
            if (connectFailure != null) {
                throw connectFailure;
            }
            return returnedConnId;
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
        private int updateCount = 3;
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
                    case "getUpdateCount" -> updateCount;
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

    private static final class JdbcMetadataRecorder {
        private String lastMethod;
        private String lastCatalog;
        private String lastSchema;
        private String lastTable;
        private String lastPattern;
        List<Map<String, Object>> tableRows = List.of();
        List<Map<String, Object>> columnRows = List.of();

        private Connection connection(String productName) {
            DatabaseMetaData meta = (DatabaseMetaData) Proxy.newProxyInstance(
                DispatcherTest.class.getClassLoader(),
                new Class<?>[]{DatabaseMetaData.class},
                (_proxy, method, args) -> switch (method.getName()) {
                    case "getDatabaseProductName" -> productName;
                    case "getTables" -> {
                        lastMethod = "getTables";
                        lastCatalog = (String) args[0];
                        lastSchema = (String) args[1];
                        lastPattern = (String) args[2];
                        yield metadataResultSet(
                            List.of("TABLE_NAME", "TABLE_TYPE", "TABLE_SCHEM", "TABLE_CAT"),
                            tableRows
                        );
                    }
                    case "getColumns" -> {
                        lastMethod = "getColumns";
                        lastCatalog = (String) args[0];
                        lastSchema = (String) args[1];
                        lastTable = (String) args[2];
                        lastPattern = (String) args[3];
                        yield metadataResultSet(
                            List.of("COLUMN_NAME", "TYPE_NAME", "NULLABLE", "ORDINAL_POSITION",
                                    "TABLE_CAT", "TABLE_SCHEM"),
                            columnRows
                        );
                    }
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

        private ResultSet metadataResultSet(List<String> columns, List<Map<String, Object>> rows) {
            final int[] index = {-1};
            final Object[] lastValue = {null};
            return (ResultSet) Proxy.newProxyInstance(
                DispatcherTest.class.getClassLoader(),
                new Class<?>[]{ResultSet.class},
                (_proxy, method, args) -> switch (method.getName()) {
                    case "next" -> ++index[0] < rows.size();
                    case "getString" -> {
                        Object value = rows.get(index[0]).get((String) args[0]);
                        lastValue[0] = value;
                        yield value == null ? null : value.toString();
                    }
                    case "getInt" -> {
                        Object value = rows.get(index[0]).get((String) args[0]);
                        lastValue[0] = value;
                        yield value == null ? 0 : ((Number) value).intValue();
                    }
                    case "getObject" -> {
                        Object value = rows.get(index[0]).get((String) args[0]);
                        lastValue[0] = value;
                        yield value;
                    }
                    case "wasNull" -> lastValue[0] == null;
                    case "getMetaData" -> resultSetMetaData(columns);
                    case "close" -> null;
                    case "unwrap" -> null;
                    case "isWrapperFor" -> false;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        }

        private ResultSetMetaData resultSetMetaData(List<String> columns) {
            return (ResultSetMetaData) Proxy.newProxyInstance(
                DispatcherTest.class.getClassLoader(),
                new Class<?>[]{ResultSetMetaData.class},
                (_proxy, method, args) -> switch (method.getName()) {
                    case "getColumnCount" -> columns.size();
                    case "getColumnLabel" -> columns.get(((Integer) args[0]) - 1);
                    case "getColumnTypeName" -> "VARCHAR";
                    case "unwrap" -> null;
                    case "isWrapperFor" -> false;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        }

        private Map<String, Object> tableRow(String name, String type, String schema, String catalog) {
            Map<String, Object> row = new HashMap<>();
            row.put("TABLE_NAME", name);
            row.put("TABLE_TYPE", type);
            row.put("TABLE_SCHEM", schema);
            row.put("TABLE_CAT", catalog);
            return row;
        }

        private Map<String, Object> columnRow(String name, String type, int nullable, int position) {
            return columnRow(name, type, nullable, position, null, null);
        }

        private Map<String, Object> columnRow(String name, String type, int nullable, int position,
                                              String catalog) {
            return columnRow(name, type, nullable, position, catalog, null);
        }

        private Map<String, Object> columnRow(String name, String type, int nullable, int position,
                                              String catalog, String schema) {
            Map<String, Object> row = new HashMap<>();
            row.put("COLUMN_NAME", name);
            row.put("TYPE_NAME", type);
            row.put("NULLABLE", nullable);
            row.put("ORDINAL_POSITION", position);
            row.put("TABLE_CAT", catalog);
            row.put("TABLE_SCHEM", schema);
            return row;
        }
    }
}
