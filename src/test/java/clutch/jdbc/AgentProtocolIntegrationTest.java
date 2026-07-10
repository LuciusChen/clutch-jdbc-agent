package clutch.jdbc;

import clutch.jdbc.handler.Dispatcher;
import clutch.jdbc.model.Request;
import clutch.jdbc.model.Response;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AgentProtocolIntegrationTest {

    @Test
    void jsonProtocolExecutesPreparedJdbcRoundTrip() throws Exception {
        Class.forName("org.h2.Driver");
        ObjectMapper mapper = new ObjectMapper();
        ConnectionManager connections = new ConnectionManager();
        Dispatcher dispatcher = new Dispatcher(connections, new CursorManager());
        try {
            JsonNode connect = roundTrip(mapper, dispatcher, """
                {"id":1,"op":"connect","params":{
                  "url":"jdbc:h2:mem:protocol;DB_CLOSE_DELAY=-1",
                  "driver-class":"org.h2.Driver",
                  "user":"sa","password":"","auto-commit":false}}
                """);
            assertTrue(connect.path("ok").asBoolean());
            int connId = connect.path("result").path("conn-id").asInt();
            assertFalse(connections.getPrimary(connId).getAutoCommit());

            assertTrue(roundTrip(mapper, dispatcher, """
                {"id":2,"op":"execute","params":{"conn-id":%d,
                  "sql":"CREATE TABLE orders (id INTEGER PRIMARY KEY, note VARCHAR(64), optional VARCHAR(64))"}}
                """.formatted(connId)).path("ok").asBoolean());

            JsonNode insert = roundTrip(mapper, dispatcher, """
                {"id":3,"op":"execute-params","params":{"conn-id":%d,
                  "sql":"INSERT INTO orders (id, note, optional) VALUES (?, ?, ?)",
                  "values":[17,"中文",null]}}
                """.formatted(connId));
            assertEquals(1, insert.path("result").path("affected-rows").asInt());

            JsonNode query = roundTrip(mapper, dispatcher, """
                {"id":4,"op":"execute","params":{"conn-id":%d,
                  "sql":"SELECT id, note, optional FROM orders"}}
                """.formatted(connId));
            JsonNode row = query.path("result").path("rows").path(0);
            assertEquals(17, row.path(0).asInt());
            assertEquals("中文", row.path(1).asText());
            assertTrue(row.path(2).isNull());
        } finally {
            connections.disconnectAll();
            dispatcher.shutdown();
        }
    }

    private JsonNode roundTrip(ObjectMapper mapper, Dispatcher dispatcher, String json)
            throws Exception {
        Request request = mapper.readValue(json, Request.class);
        Response response = dispatcher.dispatch(request);
        return mapper.readTree(mapper.writeValueAsBytes(response));
    }
}
