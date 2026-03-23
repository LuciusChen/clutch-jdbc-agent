package clutch.jdbc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TypeConverterTest {

    @Test
    void convertPropagatesClobReadFailures() {
        Clob clob = (Clob) Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[]{Clob.class},
            (_proxy, method, _args) -> switch (method.getName()) {
                case "length" -> 32L;
                case "getSubString" -> throw new SQLException("clob preview failed");
                case "free" -> null;
                case "unwrap" -> null;
                case "isWrapperFor" -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });

        SQLException err = assertThrows(SQLException.class,
            () -> TypeConverter.convert(resultSetReturning(clob), 1));
        assertTrue(err.getMessage().contains("clob preview failed"));
    }

    @Test
    void convertPropagatesBlobReadFailures() {
        Blob blob = (Blob) Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[]{Blob.class},
            (_proxy, method, _args) -> switch (method.getName()) {
                case "length" -> 12L;
                case "getBytes" -> throw new SQLException("blob read failed");
                case "free" -> null;
                case "unwrap" -> null;
                case "isWrapperFor" -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });

        SQLException err = assertThrows(SQLException.class,
            () -> TypeConverter.convert(resultSetReturning(blob), 1));
        assertTrue(err.getMessage().contains("blob read failed"));
    }

    @Test
    void blobBytesToMapIncludesJsonTextWhenUtf8PayloadLooksStructured() {
        @SuppressWarnings("unchecked")
        Map<String, Object> converted = (Map<String, Object>)
            TypeConverter.blobBytesToMap("{\"hello\":1}".getBytes(java.nio.charset.StandardCharsets.UTF_8), 11L);

        assertEquals("blob", converted.get("__type"));
        assertEquals(11L, converted.get("length"));
        assertEquals("{\"hello\":1}", converted.get("text"));
    }

    @Test
    void blobBytesToMapFallsBackToPlainPlaceholderForInvalidUtf8() {
        @SuppressWarnings("unchecked")
        Map<String, Object> converted = (Map<String, Object>)
            TypeConverter.blobBytesToMap(new byte[]{(byte) 0xC3, 0x28}, 2L);

        assertEquals("blob", converted.get("__type"));
        assertEquals(2L, converted.get("length"));
        assertFalse(converted.containsKey("text"));
    }

    @Test
    void convertKeepsTimestampInLocalWallClockFormat() throws SQLException {
        TimeZone original = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
        try {
            Object converted = TypeConverter.convert(
                resultSetReturning(Timestamp.valueOf("2024-06-28 19:30:00")),
                1);
            assertEquals("2024-06-28 19:30:00", converted);
        } finally {
            TimeZone.setDefault(original);
        }
    }

    @Test
    void convertPreservesNonZeroTimestampFractionWithoutTrailingZeros() throws SQLException {
        Object converted = TypeConverter.convert(
            resultSetReturning(Timestamp.valueOf("2024-06-28 19:30:00.120000000")),
            1);
        assertEquals("2024-06-28 19:30:00.12", converted);
    }

    @ParameterizedTest
    @MethodSource("basicTypeConversions")
    void convertHandlesBasicJdbcTypes(Object input, Object expected) throws SQLException {
        assertEquals(expected, TypeConverter.convert(resultSetReturning(input), 1));
    }

    private static Stream<Arguments> basicTypeConversions() {
        return Stream.of(
            // Boolean
            Arguments.of(true, true),
            Arguments.of(false, false),
            // Integer
            Arguments.of(42, 42),
            Arguments.of(-1, -1),
            Arguments.of(0, 0),
            Arguments.of(Integer.MAX_VALUE, Integer.MAX_VALUE),
            // Long
            Arguments.of(9_999_999_999L, 9_999_999_999L),
            Arguments.of(-9_999_999_999L, -9_999_999_999L),
            // Short → int
            Arguments.of((short) 32767, 32767),
            Arguments.of((short) -1, -1),
            // Byte → int
            Arguments.of((byte) 127, 127),
            Arguments.of((byte) -128, -128),
            // Double (normal)
            Arguments.of(3.14, 3.14),
            Arguments.of(0.0, 0.0),
            // Double (NaN/Infinity → String)
            Arguments.of(Double.NaN, "NaN"),
            Arguments.of(Double.POSITIVE_INFINITY, "Infinity"),
            Arguments.of(Double.NEGATIVE_INFINITY, "-Infinity"),
            // Float → double (normal)
            Arguments.of(2.5f, 2.5),
            // Float (NaN/Infinity → String)
            Arguments.of(Float.NaN, "NaN"),
            Arguments.of(Float.POSITIVE_INFINITY, "Infinity"),
            // BigDecimal → String (preserves precision)
            Arguments.of(new BigDecimal("123456789.123456789"), "123456789.123456789"),
            Arguments.of(new BigDecimal("0.00"), "0.00"),
            Arguments.of(new BigDecimal("1E+10"), "10000000000"),
            // Date → local date string
            Arguments.of(Date.valueOf("2024-03-15"), "2024-03-15"),
            Arguments.of(Date.valueOf("1970-01-01"), "1970-01-01"),
            // Time → wall-clock string
            Arguments.of(Time.valueOf("14:30:00"), "14:30:00"),
            Arguments.of(Time.valueOf("00:00:00"), "00:00:00")
        );
    }

    @Test
    void convertReturnsNullForSqlNull() throws SQLException {
        ResultSet rs = (ResultSet) Proxy.newProxyInstance(
            TypeConverterTest.class.getClassLoader(),
            new Class<?>[]{ResultSet.class},
            (_proxy, method, _args) -> switch (method.getName()) {
                case "getObject" -> null;
                case "wasNull" -> true;
                case "unwrap" -> null;
                case "isWrapperFor" -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });
        assertNull(TypeConverter.convert(rs, 1));
    }

    @Test
    void convertLargeBlobReturnsPlaceholderWithoutBytes() throws SQLException {
        long largeSize = 100_000L;
        Blob blob = (Blob) Proxy.newProxyInstance(
            TypeConverterTest.class.getClassLoader(),
            new Class<?>[]{Blob.class},
            (_proxy, method, _args) -> switch (method.getName()) {
                case "length" -> largeSize;
                case "getBytes" -> throw new AssertionError("should not read bytes for large blob");
                case "unwrap" -> null;
                case "isWrapperFor" -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) TypeConverter.convert(
            resultSetReturning(blob), 1);
        assertEquals("blob", result.get("__type"));
        assertEquals(largeSize, result.get("length"));
        assertFalse(result.containsKey("text"));
    }

    @Test
    void convertUnknownTypeFallsBackToGetString() throws SQLException {
        // Use an object whose type doesn't match any known branch
        Object exotic = new Object() {
            @Override public String toString() { return "exotic-value"; }
        };
        ResultSet rs = (ResultSet) Proxy.newProxyInstance(
            TypeConverterTest.class.getClassLoader(),
            new Class<?>[]{ResultSet.class},
            (_proxy, method, _args) -> switch (method.getName()) {
                case "getObject" -> exotic;
                case "wasNull" -> false;
                case "getString" -> "getString-result";
                case "unwrap" -> null;
                case "isWrapperFor" -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });
        assertEquals("getString-result", TypeConverter.convert(rs, 1));
    }

    private static ResultSet resultSetReturning(Object value) {
        return (ResultSet) Proxy.newProxyInstance(
            TypeConverterTest.class.getClassLoader(),
            new Class<?>[]{ResultSet.class},
            (_proxy, method, _args) -> switch (method.getName()) {
                case "getObject" -> value;
                case "wasNull" -> false;
                case "getString" -> value == null ? null : value.toString();
                case "unwrap" -> null;
                case "isWrapperFor" -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });
    }
}
