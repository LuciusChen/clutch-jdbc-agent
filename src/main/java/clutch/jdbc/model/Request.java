package clutch.jdbc.model;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;

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
}
