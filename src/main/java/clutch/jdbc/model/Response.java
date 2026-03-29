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

    /** Optional structured diagnostics for request failures. */
    @JsonProperty("diag")
    public Object diag;

    /** Optional verbose debug payload, only present when explicitly requested. */
    @JsonProperty("debug")
    public Object debug;

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
        return error(id, message, null);
    }

    /** Build an error response with the given message and diagnostics payload. */
    public static Response error(int id, String message, Object diagnostics) {
        return error(id, message, diagnostics, null);
    }

    /** Build an error response with the given message, diagnostics, and debug payload. */
    public static Response error(int id, String message, Object diagnostics, Object debugPayload) {
        Response r = new Response();
        r.id = id;
        r.ok = false;
        r.error = message;
        r.diag = diagnostics;
        r.debug = debugPayload;
        return r;
    }
}
