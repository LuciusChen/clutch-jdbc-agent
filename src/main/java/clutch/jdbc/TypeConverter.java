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

    public static Object convert(ResultSet rs, int col) throws SQLException {
        Object val = rs.getObject(col);
        if (rs.wasNull() || val == null) return null;

        return switch (val) {
            case Boolean b   -> b;
            case Integer i   -> i;
            case Long l      -> l;
            case Short s     -> s.intValue();
            case Byte b      -> b.intValue();

            // Double/Float: guard against NaN and Infinity which are not valid JSON.
            case Double d    -> (d.isNaN() || d.isInfinite()) ? d.toString() : d;
            case Float f     -> (f.isNaN() || f.isInfinite()) ? f.toString() : (double) f;

            // BigDecimal → String to preserve precision (avoids JS float rounding).
            case BigDecimal bd -> bd.toPlainString();

            // Date/Time → ISO-8601 strings.
            // Note: Oracle DATE has a time component; getTimestamp() is safer than getDate().
            case Timestamp ts -> ts.toInstant().toString();
            case Date d       -> d.toLocalDate().toString();        // "2024-01-15"
            case Time t       -> t.toLocalTime().toString();        // "13:45:30"

            // Large objects: return a placeholder, not the full content.
            case Clob clob -> {
                try {
                    long len = clob.length();
                    // Return first 256 chars as preview.
                    String preview = clob.getSubString(1, (int) Math.min(len, 256));
                    yield java.util.Map.of("__type", "clob", "length", len, "preview", preview);
                } catch (SQLException e) {
                    yield java.util.Map.of("__type", "clob", "error", e.getMessage());
                }
            }
            case Blob blob -> {
                try {
                    long len = blob.length();
                    if (len <= 65536) {
                        yield blobBytesToMap(blob.getBytes(1, (int) len), len);
                    }
                    yield java.util.Map.of("__type", "blob", "length", len);
                } catch (SQLException e) {
                    yield java.util.Map.of("__type", "blob", "error", e.getMessage());
                }
            }
            case byte[] bytes -> blobBytesToMap(bytes, bytes.length);

            // Fallback: getString() for anything else (e.g. Oracle-specific types).
            default -> {
                String s = rs.getString(col);
                yield s != null ? s : val.toString();
            }
        };
    }
}
