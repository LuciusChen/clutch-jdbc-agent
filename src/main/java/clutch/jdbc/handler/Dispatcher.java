package clutch.jdbc.handler;

import clutch.jdbc.ConnectionManager;
import clutch.jdbc.CursorManager;
import clutch.jdbc.model.Request;
import clutch.jdbc.model.Response;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Routes incoming requests to the appropriate handler method.
 * All methods return a Response; exceptions are caught at the Agent level.
 */
public class Dispatcher {

    private static final System.Logger LOG = System.getLogger(Dispatcher.class.getName());

    private static final int DEFAULT_FETCH_SIZE = 500;
    private static final int MAX_FETCH_SIZE = 10_000;
    static final int DEFAULT_EXECUTE_TIMEOUT = 29; // s; safety net when no client timeout given
    static final int MAX_CONCURRENT_JDBC_TASKS = 16;
    private static final String EXECUTOR_OVERLOADED_ERROR =
        "Agent overloaded: too many concurrent JDBC operations";
    private static final ObjectMapper JSON = new ObjectMapper();

    private final ConnectionManager connMgr;
    private final CursorManager cursorMgr;
    private final ExecutorService executePool = newExecutePool();
    private final Map<Integer, ReentrantLock> connectionLocks = new ConcurrentHashMap<>();
    private final Map<Integer, RunningStatement> runningStatements = new ConcurrentHashMap<>();
    private final ThreadLocal<String> generatedSqlContext = new ThreadLocal<>();
    private final DispatcherDiagnostics diagnostics;
    private final MetadataOps metadataOps;

    private record RunningStatement(int requestId, Statement statement,
                                    AtomicBoolean cancelRequestedFlag) {
        private RunningStatement(int requestId, Statement statement) {
            this(requestId, statement, new AtomicBoolean(false));
        }

        private void markCancelRequested() {
            cancelRequestedFlag.set(true);
        }

        private boolean cancelRequested() {
            return cancelRequestedFlag.get();
        }
    }

    /** Create a Dispatcher backed by the given connection and cursor managers. */
    public Dispatcher(ConnectionManager connMgr, CursorManager cursorMgr) {
        this.connMgr = connMgr;
        this.cursorMgr = cursorMgr;
        this.diagnostics = new DispatcherDiagnostics(MetadataOps.SUPPORTED_OPS, generatedSqlContext);
        this.metadataOps = new MetadataOps(connMgr, cursorMgr, generatedSqlContext);
    }

    /** Shut down the execute thread pool. Called on agent shutdown. */
    public void shutdown() {
        executePool.shutdownNow();
    }

    /** Route {@code req} to the appropriate handler and return its response. */
    public Response dispatch(Request req) {
        generatedSqlContext.remove();
        try {
            if (requestBypassesConnectionLock(req)) {
                return dispatchUnlocked(req);
            }
            Integer connId = lockConnectionId(req);
            if (connId == null) {
                return dispatchUnlocked(req);
            }
            return withConnectionLock(connId, () -> dispatchUnlocked(req));
        } catch (Exception e) {
            return errorResponse(req, e);
        } finally {
            generatedSqlContext.remove();
        }
    }

    private Response errorResponse(Request req, Exception e) {
        String message = diagnostics.requestErrorMessage(req, e);
        Integer connId = requestConnectionId(req);
        Map<String, Object> diag =
            diagnostics.requestDiagnostics(
                req, diagnostics.requestErrorCategory(req, e), e, message, connId);
        Map<String, Object> debugPayload = diagnostics.requestDebugPayload(req, e, connId);
        if ("connect".equals(req.op)) {
            LOG.log(System.Logger.Level.ERROR,
                "Connect request {0} failed: {1}",
                req.id,
                message);
        }
        return Response.error(req.id, message, diag, debugPayload);
    }

    private Response errorResponse(Request req, String message, String category) {
        return errorResponse(req, message, category, null, requestConnectionId(req));
    }

    private Response errorResponse(Request req, String message, String category,
                                   Throwable throwable, Integer connId) {
        return Response.error(
            req.id,
            message,
            diagnostics.requestDiagnostics(req, category, throwable, message, connId),
            diagnostics.requestDebugPayload(req, throwable, connId));
    }

    private boolean requestUsesDirectConnectionId(String op) {
        return switch (op) {
            case "disconnect", "commit", "rollback", "set-auto-commit",
                 "set-current-schema", "execute", "execute-params" -> true;
            default -> false;
        };
    }

    private Integer requestConnectionId(Request req) {
        Object connId = req.params.get("conn-id");
        if (connId instanceof Number number) {
            return number.intValue();
        }
        if ("fetch".equals(req.op)) {
            Object cursorId = req.params.get("cursor-id");
            if (cursorId instanceof Number number) {
                try {
                    return cursorMgr.connectionId(number.intValue());
                } catch (SQLException ignored) {
                    return null;
                }
            }
        }
        return null;
    }

    private Response dispatchUnlocked(Request req) throws Exception {
        if (MetadataOps.supports(req.op)) {
            return dispatchMetadata(req);
        }
        return switch (req.op) {
            case "ping" -> ping(req);
            case "connect" -> connect(req);
            case "disconnect" -> disconnect(req);
            case "commit" -> commit(req);
            case "rollback" -> rollback(req);
            case "set-auto-commit" -> setAutoCommit(req);
            case "cancel" -> cancel(req);
            case "execute" -> execute(req);
            case "execute-params" -> executeParams(req);
            case "fetch" -> fetch(req);
            case "close-cursor" -> closeCursor(req);
            default -> errorResponse(req, "Unknown op: " + req.op, "protocol");
        };
    }

    private Response dispatchMetadata(Request req) throws Exception {
        try {
            return metadataOps.dispatch(req);
        } catch (SQLException error) {
            Integer connId = requestConnectionId(req);
            if (connId != null && connMgr.reconnectMetadataIfInvalid(connId, error)) {
                metadataOps.restoreCurrentSchema(connId);
                return metadataOps.dispatch(req);
            }
            throw error;
        }
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
        String url = (String) req.params.get("url");
        String driverClass = (String) req.params.get("driver-class");
        String user = (String) req.params.get("user");
        String password = (String) req.params.get("password");
        Map<String, String> props =
            (Map<String, String>) req.params.getOrDefault("props", Map.of());
        Integer connectTimeoutSeconds = getOptionalInt(req, "connect-timeout-seconds");
        Integer networkTimeoutSeconds = getOptionalInt(req, "network-timeout-seconds");
        boolean autoCommit = getBoolean(req, "auto-commit", true);

        if (url == null) {
            return errorResponse(req, "connect: 'url' is required", "protocol");
        }
        if (driverClass == null || driverClass.isBlank()) {
            return errorResponse(req, "connect: 'driver-class' is required", "protocol");
        }

        int connId = connMgr.connect(url, user, password, props,
            connectTimeoutSeconds, networkTimeoutSeconds, autoCommit, driverClass);
        return Response.ok(req.id, Map.of("conn-id", connId));
    }

    private Response disconnect(Request req) throws SQLException {
        int connId = getInt(req, "conn-id");
        cursorMgr.closeForConnection(connId);
        connMgr.disconnect(connId);
        runningStatements.remove(connId);
        connectionLocks.remove(connId);
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
        boolean autoCommit = getBoolean(req, "auto-commit", true);
        primaryConnection(connId).setAutoCommit(autoCommit);
        return Response.ok(req.id, Map.of("conn-id", connId, "auto-commit", autoCommit));
    }

    private Response cancel(Request req) {
        int connId = getInt(req, "conn-id");
        RunningStatement running = runningStatements.get(connId);
        if (running == null) {
            return Response.ok(req.id, Map.of("conn-id", connId, "cancelled", false));
        }
        try {
            running.markCancelRequested();
            running.statement().cancel();
            return Response.ok(req.id, Map.of(
                "conn-id", connId,
                "request-id", running.requestId(),
                "cancelled", true));
        } catch (SQLException e) {
            return errorResponse(req, e.getMessage(), "cancel", e, connId);
        }
    }

    // -------------------------------------------------------------------------
    // Execute / Fetch / Close
    // -------------------------------------------------------------------------

    private Response execute(Request req) throws Exception {
        int connId = getInt(req, "conn-id");
        String sql = normalizedSql(req);
        int fetchSize = getFetchSize(req);
        Integer queryTimeoutSeconds = getOptionalInt(req, "query-timeout-seconds");
        int executeTimeout = (queryTimeoutSeconds != null && queryTimeoutSeconds > 0)
            ? queryTimeoutSeconds : DEFAULT_EXECUTE_TIMEOUT;

        Connection conn = primaryConnection(connId);
        if (!conn.isValid(3)) {
            return errorResponse(req,
                "Connection lost: the server closed the connection (idle timeout). Please reconnect.",
                "connection-lost", null, connId);
        }
        Statement stmt = conn.createStatement();
        return executeStatement(
            req, connId, stmt, () -> stmt.execute(sql), fetchSize, executeTimeout);
    }

    private Response executeParams(Request req) throws Exception {
        int connId = getInt(req, "conn-id");
        String sql = normalizedSql(req);
        int fetchSize = getFetchSize(req);
        Integer queryTimeoutSeconds = getOptionalInt(req, "query-timeout-seconds");
        int executeTimeout = (queryTimeoutSeconds != null && queryTimeoutSeconds > 0)
            ? queryTimeoutSeconds : DEFAULT_EXECUTE_TIMEOUT;
        Connection conn = primaryConnection(connId);
        if (!conn.isValid(3)) {
            return errorResponse(req,
                "Connection lost: the server closed the connection (idle timeout). Please reconnect.",
                "connection-lost", null, connId);
        }
        PreparedStatement stmt = conn.prepareStatement(sql);
        try {
            bindValues(req, stmt);
        } catch (Exception error) {
            closeStatementQuietly(stmt);
            throw error;
        }
        return executeStatement(
            req, connId, stmt, stmt::execute, fetchSize, executeTimeout);
    }

    private Response executeStatement(Request req, int connId, Statement stmt,
                                      Callable<Boolean> executeAction, int fetchSize,
                                      int executeTimeout) throws Exception {
        RunningStatement running = null;
        Integer cursorId = null;
        boolean cursorResponseReady = false;
        try {
            stmt.setQueryTimeout(executeTimeout);
            running = beginRunningStatement(connId, req.id, stmt);
            Future<Boolean> future;
            try {
                future = executePool.submit(executeAction);
            } catch (RejectedExecutionException e) {
                return errorResponse(req, EXECUTOR_OVERLOADED_ERROR, "internal", e, connId);
            }
            boolean isQuery;
            try {
                isQuery = future.get(executeTimeout + 1L, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                future.cancel(true);
                cancelStatementQuietly(stmt);
                return errorResponse(req, "Query timed out after " + executeTimeout + "s",
                    "timeout", e, connId);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (running.cancelRequested() && cause instanceof SQLException sqlException) {
                    return errorResponse(req, sqlException.getMessage(), "cancel", sqlException, connId);
                }
                throw (cause instanceof Exception ex) ? ex : new RuntimeException(cause);
            }

            if (!isQuery) {
                int affected = stmt.getUpdateCount();
                return Response.ok(req.id, Map.of("type", "dml", "affected-rows", affected));
            }

            ResultSet rs = stmt.getResultSet();
            rs.setFetchSize(fetchSize);
            cursorId = cursorMgr.register(connId, stmt, rs);
            CursorManager.FetchResult first;
            try {
                first = fetchCursorBatch(cursorId, fetchSize, stmt, executeTimeout);
            } catch (SQLException e) {
                return errorResponse(req, e.getMessage(),
                    fetchFailureCategory(running, e), e, connId);
            }
            Map<String, Object> result = new java.util.LinkedHashMap<>();
            result.put("type", "query");
            result.put("cursor-id", first.done() ? null : cursorId);
            result.put("columns", first.columns());
            result.put("col-types", first.types());
            result.put("rows", first.rows());
            result.put("done", first.done());
            cursorResponseReady = true;
            return Response.ok(req.id, result);
        } finally {
            if (running != null) {
                finishRunningStatement(connId, running);
            }
            if (cursorId == null) {
                closeStatementQuietly(stmt);
            } else if (!cursorResponseReady) {
                cursorMgr.close(cursorId);
            }
        }
    }

    private String normalizedSql(Request req) {
        return getString(req, "sql").stripTrailing().replaceAll(";+$", "");
    }

    private void bindValues(Request req, PreparedStatement stmt) throws SQLException {
        Object rawValues = req.params.get("values");
        if (!(rawValues instanceof List<?> values)) {
            throw new IllegalArgumentException("execute-params: 'values' must be an array");
        }
        for (int i = 0; i < values.size(); i++) {
            stmt.setObject(i + 1, jdbcValue(values.get(i)));
        }
    }

    private Object jdbcValue(Object value) throws SQLException {
        if (!(value instanceof Map<?, ?>) && !(value instanceof List<?>)) {
            return value;
        }
        try {
            return JSON.writeValueAsString(value);
        } catch (JsonProcessingException error) {
            throw new SQLException("Cannot serialize structured JDBC parameter", error);
        }
    }

    private Response fetch(Request req) throws Exception {
        int cursorId = getInt(req, "cursor-id");
        int fetchSize = getFetchSize(req);
        Integer queryTimeoutSeconds = getOptionalInt(req, "query-timeout-seconds");
        int fetchTimeout = (queryTimeoutSeconds != null && queryTimeoutSeconds > 0)
            ? queryTimeoutSeconds : DEFAULT_EXECUTE_TIMEOUT;
        int connId = cursorMgr.connectionId(cursorId);
        Statement stmt = cursorMgr.statement(cursorId);
        RunningStatement running = beginRunningStatement(connId, req.id, stmt);
        try {
            try {
                CursorManager.FetchResult fr = fetchCursorBatch(cursorId, fetchSize, stmt, fetchTimeout);
                Map<String, Object> result = new java.util.LinkedHashMap<>();
                result.put("cursor-id", fr.done() ? null : cursorId);
                result.put("rows", fr.rows());
                result.put("done", fr.done());
                return Response.ok(req.id, result);
            } catch (SQLException e) {
                return errorResponse(req, e.getMessage(),
                    fetchFailureCategory(running, e), e, connId);
            }
        } finally {
            finishRunningStatement(connId, running);
        }
    }

    private Response closeCursor(Request req) {
        int cursorId = getInt(req, "cursor-id");
        cursorMgr.close(cursorId);
        return Response.ok(req.id, Map.of("cursor-id", cursorId));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private Connection primaryConnection(int connId) throws SQLException {
        return connMgr.getPrimary(connId);
    }

    private boolean requestBypassesConnectionLock(Request req) {
        return switch (req.op) {
            case "ping", "connect", "cancel", "close-cursor" -> true;
            default -> false;
        };
    }

    private Integer lockConnectionId(Request req) throws SQLException {
        if (requestUsesDirectConnectionId(req.op)) {
            return getInt(req, "conn-id");
        }
        return switch (req.op) {
            case "fetch" -> cursorMgr.connectionId(getInt(req, "cursor-id"));
            default -> null;
        };
    }

    private Response withConnectionLock(int connId, Callable<Response> action) throws Exception {
        ReentrantLock lock = connectionLocks.computeIfAbsent(connId, _id -> new ReentrantLock());
        lock.lock();
        try {
            return action.call();
        } finally {
            lock.unlock();
        }
    }

    private RunningStatement beginRunningStatement(int connId, int requestId, Statement stmt) {
        RunningStatement running = new RunningStatement(requestId, stmt);
        RunningStatement previous = runningStatements.putIfAbsent(connId, running);
        if (previous != null) {
            throw new IllegalStateException("Connection " + connId + " already has a running statement");
        }
        return running;
    }

    private void finishRunningStatement(int connId, RunningStatement running) {
        runningStatements.remove(connId, running);
    }

    private CursorManager.FetchResult fetchCursorBatch(int cursorId, int fetchSize,
                                                       Statement stmt, int timeoutSeconds)
            throws Exception {
        Future<CursorManager.FetchResult> future;
        try {
            future = executePool.submit(() -> cursorMgr.fetch(cursorId, fetchSize));
        } catch (RejectedExecutionException e) {
            cursorMgr.close(cursorId);
            throw new SQLException(EXECUTOR_OVERLOADED_ERROR);
        }
        try {
            return future.get(timeoutSeconds + 1L, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            cancelStatementQuietly(stmt);
            cursorMgr.close(cursorId);
            throw new SQLTimeoutException("Query timed out after " + timeoutSeconds + "s");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            cursorMgr.close(cursorId);
            if (cause instanceof SQLException sqlException) {
                throw sqlException;
            }
            throw (cause instanceof Exception ex) ? ex : new RuntimeException(cause);
        }
    }

    private static ExecutorService newExecutePool() {
        ThreadPoolExecutor pool = new ThreadPoolExecutor(
            MAX_CONCURRENT_JDBC_TASKS,
            MAX_CONCURRENT_JDBC_TASKS,
            60L,
            TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            runnable -> {
                Thread thread = new Thread(runnable, "clutch-jdbc-execute");
                thread.setDaemon(true);
                return thread;
            });
        pool.allowCoreThreadTimeOut(true);
        return pool;
    }

    private String fetchFailureCategory(RunningStatement running, SQLException error) {
        if (running != null && running.cancelRequested()) {
            return "cancel";
        }
        if (error instanceof SQLTimeoutException) {
            return "timeout";
        }
        return "fetch";
    }

    private int getInt(Request req, String key) {
        Object value = req.params.get(key);
        if (value instanceof Number number) {
            return number.intValue();
        }
        throw new IllegalArgumentException("Missing or non-integer param: " + key);
    }

    private String getString(Request req, String key) {
        Object value = req.params.get(key);
        if (value instanceof String string) {
            return string;
        }
        throw new IllegalArgumentException("Missing or non-string param: " + key);
    }

    private Integer getOptionalInt(Request req, String key) {
        Object value = req.params.get(key);
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        throw new IllegalArgumentException("Non-integer param: " + key);
    }

    private int getFetchSize(Request req) {
        if (!req.params.containsKey("fetch-size")) {
            return DEFAULT_FETCH_SIZE;
        }
        Object value = req.params.get("fetch-size");
        if (!(value instanceof Byte || value instanceof Short
              || value instanceof Integer || value instanceof Long)) {
            throw new IllegalArgumentException("Non-integer param: fetch-size");
        }
        long fetchSize = ((Number) value).longValue();
        if (fetchSize < 1 || fetchSize > MAX_FETCH_SIZE) {
            throw new IllegalArgumentException(
                "fetch-size must be between 1 and " + MAX_FETCH_SIZE);
        }
        return (int) fetchSize;
    }

    private boolean getBoolean(Request req, String key, boolean defaultValue) {
        Object value = req.params.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Boolean bool) {
            return bool;
        }
        throw new IllegalArgumentException("Non-boolean param: " + key);
    }

    private void cancelStatementQuietly(Statement stmt) {
        try {
            stmt.cancel();
        } catch (Exception e) {
            LOG.log(System.Logger.Level.WARNING,
                "Failed to cancel JDBC statement after timeout", e);
        }
    }

    private void closeStatementQuietly(Statement stmt) {
        try {
            stmt.close();
        } catch (Exception e) {
            LOG.log(System.Logger.Level.WARNING,
                "Failed to close JDBC statement during cleanup", e);
        }
    }
}
