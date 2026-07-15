package clutch.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

/**
 * Converts a single JDBC column value to a JSON-safe Java object.
 *
 * Design principle: stability over perfect typing.
 * Emacs receives a predictable set of Java types that Jackson can serialize:
 *   null, Boolean, Integer, Long, Double, String, Map.
 *
 * Problematic types are converted to String to avoid surprises.
 */
public class TypeConverter {

    private static final ObjectMapper JSON_VALIDATOR = new ObjectMapper();
    private static final List<Charset> BLOB_TEXT_CHARSETS =
        List.of(StandardCharsets.UTF_8, Charset.forName("GB18030"));
    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter TIME_FORMATTER =
        DateTimeFormatter.ofPattern("HH:mm:ss");

    /**
     * Convert a small BLOB's raw bytes to a map preserving the blob origin.
     * If the bytes decode as supported text and look like JSON/XML, includes
     * the text content and detected encoding.  Otherwise returns a placeholder.
     */
    static Map<String, Object> blobBytesToMap(byte[] bytes, long length) {
        for (Charset charset : BLOB_TEXT_CHARSETS) {
            String text = decodeBlobText(bytes, charset);
            if (text != null && structuredBlobText(text)) {
                return Map.of("__type", "blob",
                              "length", length,
                              "text", text,
                              "encoding", charset.name());
            }
        }
        return Map.of("__type", "blob", "length", length);
    }

    private static String decodeBlobText(byte[] bytes, Charset charset) {
        try {
            return charset.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(bytes))
                .toString()
                .strip();
        } catch (CharacterCodingException e) {
            return null;
        }
    }

    private static boolean structuredBlobText(String text) {
        if (text.isEmpty()) return false;
        return looksLikeJson(text) || looksLikeXml(text);
    }

    private static boolean looksLikeJson(String text) {
        char first = text.charAt(0);
        if (first != '{' && first != '[') return false;
        try {
            JSON_VALIDATOR.readTree(text);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private static boolean looksLikeXml(String text) {
        return text.charAt(0) == '<' && text.indexOf('>') > 1;
    }

    /**
     * Convert column {@code col} of the current {@code rs} row to a JSON-safe value.
     * Returns {@code null} for SQL NULL, or a Java type that Jackson serializes cleanly.
     */
    public static Object convert(ResultSet rs, int col) throws SQLException {
        Object val = rs.getObject(col);
        if (rs.wasNull() || val == null) return null;

        if (val instanceof Boolean) return val;
        if (val instanceof String) return val;
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

        // Date/Time -> local wall-clock strings.
        // Note: Oracle DATE has a time component; getTimestamp() is safer than getDate().
        if (val instanceof Timestamp) return formatTimestamp((Timestamp) val);
        if (val instanceof Date) return ((Date) val).toLocalDate().toString();
        if (val instanceof Time) return formatTime((Time) val);

        // Large objects: return a placeholder, not the full content.
        if (val instanceof Clob) {
            Clob clob = (Clob) val;
            long len = clob.length();
            String preview = clob.getSubString(1, (int) Math.min(len, 256));
            return java.util.Map.of("__type", "clob", "length", len, "preview", preview);
        }
        if (val instanceof Blob) {
            Blob blob = (Blob) val;
            long len = blob.length();
            if (len <= 65536) {
                return blobBytesToMap(blob.getBytes(1, (int) len), len);
            }
            return java.util.Map.of("__type", "blob", "length", len);
        }
        if (val instanceof byte[]) return blobBytesToMap((byte[]) val, ((byte[]) val).length);

        // Fallback: getString() for anything else (e.g. Oracle-specific types).
        String s = rs.getString(col);
        return s != null ? s : val.toString();
    }

    private static String formatTimestamp(Timestamp value) {
        LocalDateTime timestamp = value.toLocalDateTime();
        return formatFractionalValue(TIMESTAMP_FORMATTER.format(timestamp), timestamp.getNano());
    }

    private static String formatTime(Time value) {
        LocalTime time = value.toLocalTime();
        return formatFractionalValue(TIME_FORMATTER.format(time), time.getNano());
    }

    private static String formatFractionalValue(String base, int nanos) {
        if (nanos == 0) {
            return base;
        }
        String fraction = String.format("%09d", nanos).replaceFirst("0+$", "");
        return base + "." + fraction;
    }
}
