package clutch.jdbc;

import java.math.BigDecimal;
import java.sql.*;

/**
 * Converts a single JDBC column value to a JSON-safe Java object.
 *
 * Design principle: stability over perfect typing.
 * Emacs receives a predictable set of Java types that Jackson can serialize:
 *   null, Boolean, Integer, Long, Double, String.
 *
 * Problematic types are converted to String to avoid surprises.
 */
public class TypeConverter {

    /**
     * Convert a small BLOB's raw bytes to a map preserving the blob origin.
     * If the bytes are valid UTF-8 and look like JSON, includes the text content.
     * Otherwise returns a plain blob placeholder.
     */
    static java.util.Map<String, Object> blobBytesToMap(byte[] bytes, long length) {
        try {
            java.nio.charset.CharsetDecoder decoder =
                java.nio.charset.StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(java.nio.charset.CodingErrorAction.REPORT)
                    .onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT);
            String text = decoder.decode(java.nio.ByteBuffer.wrap(bytes)).toString().strip();
            if (!text.isEmpty() && (text.charAt(0) == '{' || text.charAt(0) == '[')) {
                return java.util.Map.of("__type", "blob", "length", length,
                                        "text", text);
            }
        } catch (java.nio.charset.CharacterCodingException ignored) {
            // Not valid UTF-8: fall through to plain blob placeholder.
        }
        return java.util.Map.of("__type", "blob", "length", length);
    }

    /**
     * Convert column {@code col} of the current {@code rs} row to a JSON-safe value.
     * Returns {@code null} for SQL NULL, or a Java type that Jackson serializes cleanly.
     */
    public static Object convert(ResultSet rs, int col) throws SQLException {
        Object val = rs.getObject(col);
        if (rs.wasNull() || val == null) return null;

        if (val instanceof Boolean) return val;
        if (val instanceof Integer) return val;
        if (val instanceof Long) return val;
        if (val instanceof Short) return ((Short) val).intValue();
        if (val instanceof Byte) return ((Byte) val).intValue();

        // Double/Float: guard against NaN and Infinity which are not valid JSON.
        if (val instanceof Double) {
            Double d = (Double) val;
            return (d.isNaN() || d.isInfinite()) ? d.toString() : d;
        }
        if (val instanceof Float) {
            Float f = (Float) val;
            return (f.isNaN() || f.isInfinite()) ? f.toString() : (double) f;
        }

        // BigDecimal → String to preserve precision (avoids JS float rounding).
        if (val instanceof BigDecimal) return ((BigDecimal) val).toPlainString();

        // Date/Time → ISO-8601 strings.
        // Note: Oracle DATE has a time component; getTimestamp() is safer than getDate().
        if (val instanceof Timestamp) return ((Timestamp) val).toInstant().toString();
        if (val instanceof Date) return ((Date) val).toLocalDate().toString();
        if (val instanceof Time) return ((Time) val).toLocalTime().toString();

        // Large objects: return a placeholder, not the full content.
        if (val instanceof Clob) {
            Clob clob = (Clob) val;
            try {
                long len = clob.length();
                String preview = clob.getSubString(1, (int) Math.min(len, 256));
                return java.util.Map.of("__type", "clob", "length", len, "preview", preview);
            } catch (SQLException e) {
                return java.util.Map.of("__type", "clob", "error", e.getMessage());
            }
        }
        if (val instanceof Blob) {
            Blob blob = (Blob) val;
            try {
                long len = blob.length();
                if (len <= 65536) {
                    return blobBytesToMap(blob.getBytes(1, (int) len), len);
                }
                return java.util.Map.of("__type", "blob", "length", len);
            } catch (SQLException e) {
                return java.util.Map.of("__type", "blob", "error", e.getMessage());
            }
        }
        if (val instanceof byte[]) return blobBytesToMap((byte[]) val, ((byte[]) val).length);

        // Fallback: getString() for anything else (e.g. Oracle-specific types).
        String s = rs.getString(col);
        return s != null ? s : val.toString();
    }
}
