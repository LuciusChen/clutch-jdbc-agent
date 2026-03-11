package clutch.jdbc.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * A single JSON response written to stdout.
 * One line of JSON per response.
 *
 * Success:  {"id":1,"ok":true,"result":{...}}
 * Error:    {"id":1,"ok":false,"error":"message"}
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Response {

    @JsonProperty("id")
    public int id;

    @JsonProperty("ok")
    public boolean ok;

    /** Present on success. */
    @JsonProperty("result")
    public Object result;

    /** Present on error. */
    @JsonProperty("error")
    public String error;

    /** Build a success response with the given result payload. */
    public static Response ok(int id, Object result) {
        Response r = new Response();
        r.id = id;
        r.ok = true;
        r.result = result;
        return r;
    }

    /** Build an error response with the given message. */
    public static Response error(int id, String message) {
        Response r = new Response();
        r.id = id;
        r.ok = false;
        r.error = message;
        return r;
    }
}
