package clutch.jdbc;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
