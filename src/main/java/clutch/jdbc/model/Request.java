package clutch.jdbc.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

/**
 * A single JSON-RPC-like request from Emacs.
 * One line of JSON per request on stdin.
 *
 * Example:
 *   {"id":1,"op":"connect","params":{"url":"jdbc:oracle:thin:@host:1521/ORCL","user":"scott","password":"tiger"}}
 */
public class Request {
    @JsonProperty("id")
    public int id;

    @JsonProperty("op")
    public String op;

    @JsonProperty("params")
    public Map<String, Object> params = new HashMap<>();

    /** Return a required exact signed 32-bit integer parameter. */
    public int getInt(String key) {
        Integer exact = intOrNull(key);
        if (exact != null) {
            return exact;
        }
        throw new IllegalArgumentException("Missing or non-integer param: " + key);
    }

    /** Return a required string parameter, including an empty string. */
    public String getString(String key) {
        Object value = params.get(key);
        if (value instanceof String string) {
            return string;
        }
        throw new IllegalArgumentException("Missing or non-string param: " + key);
    }

    /** Return an optional exact integer; missing and explicit null remain null. */
    public Integer getOptionalInt(String key) {
        if (params.get(key) == null) {
            return null;
        }
        Integer exact = intOrNull(key);
        if (exact != null) {
            return exact;
        }
        throw new IllegalArgumentException("Non-integer param: " + key);
    }

    /** Return an exact integer, or null for absent/invalid diagnostic context. */
    public Integer intOrNull(String key) {
        Object value = params.get(key);
        if (value instanceof Byte || value instanceof Short || value instanceof Integer) {
            return ((Number) value).intValue();
        }
        if (value instanceof Long longValue) {
            return longValue >= Integer.MIN_VALUE && longValue <= Integer.MAX_VALUE
                ? longValue.intValue() : null;
        }
        if (value instanceof BigInteger bigInteger
            && bigInteger.compareTo(BigInteger.valueOf(Integer.MIN_VALUE)) >= 0
            && bigInteger.compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) <= 0) {
            return bigInteger.intValue();
        }
        return null;
    }
}
