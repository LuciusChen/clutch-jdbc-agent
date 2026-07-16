package clutch.jdbc.handler;

import clutch.jdbc.model.Request;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class DispatcherDiagnostics {

    private record SecretBinding(String key, String value) {}

    /** Minimum secret value length for the conservative boundary-aware fallback. */
    private static final int MIN_SECRET_FALLBACK_LENGTH = 8;

    /**
     * Matches generic key=value or key: value fragments so the key can be
     * classified before redaction.
     */
    private static final Pattern STRUCTURED_ASSIGNMENT_PATTERN = Pattern.compile(
        "\\b([A-Za-z0-9_.-]+)(\\s*[=:]\\s*)([^&;,\\s]+)");

    private static final Pattern ORACLE_EMBEDDED_CREDENTIAL_PATTERN = Pattern.compile(
        "(?i)^(jdbc:oracle:[^:]+:)([^/@\\s]+)/([^@\\s]+)(@.*)$");

    private static final Pattern URL_ASSIGNMENT_PATTERN = Pattern.compile(
        "([?&;:])([A-Za-z0-9_.-]+)(\\s*=\\s*)([^&;\\s]*)");

    private static final Set<String> SECRET_PARAM_SUFFIXES = Set.of(
        "password", "passwd", "pwd",
        "secret",
        "token", "accesstoken",
        "cookie",
        "authorization",
        "credential", "credentials");

    private final Set<String> metadataOps;
    private final ThreadLocal<String> generatedSqlContext;
    private final ThreadLocal<Boolean> executionNotStartedContext;

    DispatcherDiagnostics(Set<String> metadataOps, ThreadLocal<String> generatedSqlContext,
                          ThreadLocal<Boolean> executionNotStartedContext) {
        this.metadataOps = metadataOps;
        this.generatedSqlContext = generatedSqlContext;
        this.executionNotStartedContext = executionNotStartedContext;
    }

    String requestErrorMessage(Request req, Exception e) {
        if ("connect".equals(req.op)) {
            return formatConnectException(req, e);
        }
        return throwableMessage(req, e);
    }

    String requestErrorCategory(Request req, Throwable throwable) {
        if (throwable instanceof IllegalArgumentException) {
            return "protocol";
        }
        if (isMetadataOperation(req.op)) {
            return "metadata";
        }
        return switch (req.op) {
            case "connect" -> "connect";
            case "execute", "execute-params" -> "query";
            case "fetch" -> "fetch";
            case "cancel" -> "cancel";
            default -> "internal";
        };
    }

    Map<String, Object> requestDiagnostics(Request req, String category,
                                           Throwable throwable, String rawMessage,
                                           Integer connId, boolean connectionInvalidated) {
        String raw = (rawMessage != null && !rawMessage.isBlank())
            ? rawMessage
            : throwableMessage(req, throwable);
        Map<String, Object> context = new LinkedHashMap<>(requestContext(req, connId));
        if (currentGeneratedSql() != null) {
            context.put("generated-sql", currentGeneratedSql());
        }
        List<Map<String, Object>> causeChain = diagnosticCauseChain(req, throwable);
        return entryMap(
            "category", category,
            "op", req.op,
            "request-id", req.id,
            "conn-id", connId,
            "exception-class", throwable != null ? throwable.getClass().getName() : null,
            "sql-state", sqlState(throwable),
            "vendor-code", vendorCode(throwable),
            "connection-invalidated", connectionInvalidated ? Boolean.TRUE : null,
            "execution-not-started",
                connectionInvalidated && Boolean.TRUE.equals(executionNotStartedContext.get())
                    ? Boolean.TRUE : null,
            "raw-message", raw,
            "cause-chain", causeChain.isEmpty() ? null : causeChain,
            "context", context.isEmpty() ? null : context);
    }

    Map<String, Object> requestDebugPayload(Request req, Throwable throwable, Integer connId) {
        if (!debugRequested(req)) {
            return null;
        }
        Map<String, Object> requestContext = new LinkedHashMap<>(requestContext(req, connId));
        if (currentGeneratedSql() != null) {
            requestContext.put("generated-sql", currentGeneratedSql());
        }
        return entryMap(
            "thread", Thread.currentThread().getName(),
            "request-context", requestContext.isEmpty() ? null : requestContext,
            "stack-trace", sanitizedStackTrace(req, throwable));
    }

    private boolean isMetadataOperation(String op) {
        return metadataOps.contains(op);
    }

    private String formatConnectException(Request req, Throwable throwable) {
        List<String> parts = new ArrayList<>();
        Throwable current = throwable;
        while (current != null && parts.size() < 4) {
            String rendered = renderConnectThrowable(req, current);
            if (!parts.contains(rendered)) {
                parts.add(rendered);
            }
            current = current.getCause();
        }
        return sanitizeVisibleText(req, String.join(" <- ", parts));
    }

    private String renderConnectThrowable(Request req, Throwable throwable) {
        StringBuilder rendered = new StringBuilder(throwable.getClass().getSimpleName());
        if (throwable instanceof SQLException sqlException) {
            if (sqlException.getSQLState() != null && !sqlException.getSQLState().isBlank()) {
                rendered.append(" [SQLState=").append(sqlException.getSQLState()).append("]");
            }
            if (sqlException.getErrorCode() != 0) {
                rendered.append(" [ErrorCode=").append(sqlException.getErrorCode()).append("]");
            }
        }
        if (throwable.getMessage() != null && !throwable.getMessage().isBlank()) {
            rendered.append(": ").append(sanitizeVisibleText(req, throwable.getMessage()));
        }
        return rendered.toString();
    }

    private List<Map<String, Object>> diagnosticCauseChain(Request req, Throwable throwable) {
        List<Map<String, Object>> causeChain = new ArrayList<>();
        Throwable current = throwable;
        while (current != null && causeChain.size() < 4) {
            causeChain.add(entryMap(
                "exception-class", current.getClass().getName(),
                "message", throwableMessage(req, current),
                "sql-state", sqlState(current),
                "vendor-code", vendorCode(current)));
            current = current.getCause();
        }
        return causeChain;
    }

    private Map<String, Object> requestContext(Request req, Integer connId) {
        if ("connect".equals(req.op)) {
            @SuppressWarnings("unchecked")
            Map<String, String> props = (Map<String, String>) req.params.get("props");
            List<String> propertyKeys = null;
            if (props != null && !props.isEmpty()) {
                propertyKeys = new ArrayList<>(props.keySet());
                propertyKeys.sort(String::compareTo);
            }
            return entryMap(
                "redacted-url", redactJdbcUrl((String) req.params.get("url")),
                "user", nonBlankString((String) req.params.get("user")),
                "property-keys", propertyKeys,
                "connect-timeout-seconds", optionalIntParam(req, "connect-timeout-seconds"),
                "network-timeout-seconds", optionalIntParam(req, "network-timeout-seconds"));
        }
        if ("execute".equals(req.op) || "execute-params".equals(req.op)) {
            String sql = (String) req.params.get("sql");
            return entryMap(
                "sql-length", sql != null ? sql.length() : null,
                "fetch-size", optionalIntParam(req, "fetch-size"),
                "query-timeout-seconds", optionalIntParam(req, "query-timeout-seconds"));
        }
        if ("fetch".equals(req.op)) {
            return entryMap(
                "cursor-id", optionalIntParam(req, "cursor-id"),
                "fetch-size", optionalIntParam(req, "fetch-size"),
                "query-timeout-seconds", optionalIntParam(req, "query-timeout-seconds"));
        }
        if ("cancel".equals(req.op)) {
            return entryMap("target-conn-id", connId != null ? connId : optionalIntParam(req, "conn-id"));
        }
        if (isMetadataOperation(req.op)) {
            return entryMap(
                "catalog", nonBlankString(optionalStringParam(req, "catalog")),
                "schema", nonBlankString(optionalStringParam(req, "schema")),
                "table", nonBlankString(optionalStringParam(req, "table")),
                "name", nonBlankString(optionalStringParam(req, "name")),
                "type", nonBlankString(optionalStringParam(req, "type")),
                "prefix", nonBlankString(optionalStringParam(req, "prefix")),
                "identity", nonBlankString(optionalStringParam(req, "identity")));
        }
        return Map.of();
    }

    private Integer optionalIntParam(Request req, String key) {
        Object value = req.params.get(key);
        if (value instanceof Byte || value instanceof Short || value instanceof Integer) {
            return ((Number) value).intValue();
        }
        if (value instanceof Long longValue
            && longValue >= Integer.MIN_VALUE && longValue <= Integer.MAX_VALUE) {
            return longValue.intValue();
        }
        if (value instanceof java.math.BigInteger bigInteger
            && bigInteger.compareTo(java.math.BigInteger.valueOf(Integer.MIN_VALUE)) >= 0
            && bigInteger.compareTo(java.math.BigInteger.valueOf(Integer.MAX_VALUE)) <= 0) {
            return bigInteger.intValue();
        }
        return null;
    }

    private boolean debugRequested(Request req) {
        return Boolean.TRUE.equals(req.params.get("debug"));
    }

    private String optionalStringParam(Request req, String key) {
        Object value = req.params.get(key);
        return value instanceof String string ? string : null;
    }

    private String currentGeneratedSql() {
        return generatedSqlContext.get();
    }

    private String throwableMessage(Request req, Throwable throwable) {
        if (throwable == null || throwable.getMessage() == null || throwable.getMessage().isBlank()) {
            return null;
        }
        return sanitizeVisibleText(req, throwable.getMessage());
    }

    private String sanitizedStackTrace(Request req, Throwable throwable) {
        if (throwable == null) {
            return null;
        }
        StringWriter writer = new StringWriter();
        throwable.printStackTrace(new PrintWriter(writer));
        return sanitizeVisibleText(req, writer.toString().strip());
    }

    private String sqlState(Throwable throwable) {
        if (throwable instanceof SQLException sqlException
            && sqlException.getSQLState() != null
            && !sqlException.getSQLState().isBlank()) {
            return sqlException.getSQLState();
        }
        return null;
    }

    private Integer vendorCode(Throwable throwable) {
        if (throwable instanceof SQLException sqlException && sqlException.getErrorCode() != 0) {
            return sqlException.getErrorCode();
        }
        return null;
    }

    private String nonBlankString(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return value;
    }

    private String redactJdbcUrl(String url) {
        if (url == null) {
            return null;
        }
        Matcher assignments = URL_ASSIGNMENT_PATTERN.matcher(url);
        StringBuffer rendered = new StringBuffer();
        while (assignments.find()) {
            String replacement = assignments.group(0);
            if (isSecretParameterName(assignments.group(2))) {
                replacement = assignments.group(1) + assignments.group(2)
                    + assignments.group(3) + "<redacted>";
            }
            assignments.appendReplacement(rendered, Matcher.quoteReplacement(replacement));
        }
        assignments.appendTail(rendered);
        String redacted = rendered.toString();
        Matcher oracleCredentials = ORACLE_EMBEDDED_CREDENTIAL_PATTERN.matcher(redacted);
        if (oracleCredentials.matches()) {
            return oracleCredentials.group(1) + oracleCredentials.group(2)
                + "/<redacted>" + oracleCredentials.group(4);
        }
        return redacted;
    }

    /**
     * Sanitizes visible text without over-redacting diagnostic information.
     */
    private String sanitizeVisibleText(Request req, String text) {
        if (text == null || text.isBlank()) {
            return text;
        }
        String sanitized = text;
        String url = optionalStringParam(req, "url");
        if (url != null && !url.isBlank()) {
            sanitized = sanitized.replace(url, redactJdbcUrl(url));
        }
        sanitized = redactStructuredSecretAssignments(sanitized);
        for (SecretBinding binding : secretRequestBindings(req)) {
            if (binding.value.length() >= MIN_SECRET_FALLBACK_LENGTH) {
                sanitized = redactAtBoundaries(sanitized, binding.value);
            }
        }
        return sanitized;
    }

    private String redactStructuredSecretAssignments(String text) {
        Matcher matcher = STRUCTURED_ASSIGNMENT_PATTERN.matcher(text);
        StringBuffer rendered = new StringBuffer();
        boolean changed = false;
        while (matcher.find()) {
            String key = matcher.group(1);
            String replacement = matcher.group(0);
            if (isSecretParameterName(key)) {
                replacement = key + matcher.group(2) + "<redacted>";
                changed = true;
            }
            matcher.appendReplacement(rendered, Matcher.quoteReplacement(replacement));
        }
        if (!changed) {
            return text;
        }
        matcher.appendTail(rendered);
        return rendered.toString();
    }

    private String redactAtBoundaries(String text, String value) {
        int index = 0;
        StringBuilder result = null;
        while (index < text.length()) {
            int found = text.indexOf(value, index);
            if (found < 0) {
                break;
            }
            boolean leftOk = found == 0 || !isIdentifierChar(text.charAt(found - 1));
            int afterIndex = found + value.length();
            boolean rightOk = afterIndex >= text.length()
                || !isIdentifierChar(text.charAt(afterIndex));
            if (leftOk && rightOk) {
                if (result == null) {
                    result = new StringBuilder(text.length());
                    result.append(text, 0, found);
                } else {
                    result.append(text, index, found);
                }
                result.append("<redacted>");
                index = afterIndex;
            } else {
                index = found + 1;
            }
        }
        if (result == null) {
            return text;
        }
        result.append(text, index, text.length());
        return result.toString();
    }

    private static boolean isIdentifierChar(char c) {
        return Character.isLetterOrDigit(c) || c == '_' || c == '-';
    }

    private List<SecretBinding> secretRequestBindings(Request req) {
        if (!"connect".equals(req.op)) {
            return List.of();
        }
        List<SecretBinding> bindings = new ArrayList<>();
        String password = optionalStringParam(req, "password");
        if (password != null && !password.isBlank()) {
            bindings.add(new SecretBinding("password", password));
        }
        String url = optionalStringParam(req, "url");
        if (url != null && !url.isBlank()) {
            bindings.addAll(secretUrlBindings(url));
        }
        Object propsObject = req.params.get("props");
        if (propsObject instanceof Map<?, ?> props) {
            for (Map.Entry<?, ?> entry : props.entrySet()) {
                if (!(entry.getKey() instanceof String key) || !(entry.getValue() instanceof String value)) {
                    continue;
                }
                if (isSecretParameterName(key) && !value.isBlank()) {
                    bindings.add(new SecretBinding(key, value));
                }
            }
        }
        return bindings;
    }

    private List<SecretBinding> secretUrlBindings(String url) {
        List<SecretBinding> bindings = new ArrayList<>();
        Matcher assignments = URL_ASSIGNMENT_PATTERN.matcher(url);
        while (assignments.find()) {
            String key = assignments.group(2);
            String value = assignments.group(4);
            if (isSecretParameterName(key) && !value.isBlank()) {
                bindings.add(new SecretBinding(key, value));
            }
        }
        Matcher oracleCredentials = ORACLE_EMBEDDED_CREDENTIAL_PATTERN.matcher(url);
        if (oracleCredentials.matches()) {
            bindings.add(new SecretBinding("password", oracleCredentials.group(3)));
        }
        return bindings;
    }

    private boolean isSecretParameterName(String key) {
        String normalized = key.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]", "");
        for (String suffix : SECRET_PARAM_SUFFIXES) {
            if (normalized.equals(suffix) || normalized.endsWith(suffix)) {
                return true;
            }
        }
        return false;
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
}
