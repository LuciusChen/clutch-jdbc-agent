package clutch.jdbc;

import clutch.jdbc.handler.Dispatcher;
import clutch.jdbc.model.Request;
import clutch.jdbc.model.Response;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertThrows;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AgentProtocolIntegrationTest {

    @Test
    void serveWritesOneParseableAtomicLinePerRequest() throws Exception {
        // Drive the production loop -- serve, writeLine, the stdout lock,
        // and the real request pool -- with more requests than pool slots.
        // Every request must come back as exactly one parseable line with a
        // unique id (dispatcher error or overload rejection both qualify);
        // an interleaved or split line would fail JSON parsing.
        int requests = 100;
        StringBuilder input = new StringBuilder();
        for (int i = 1; i <= requests; i++) {
            input.append("{\"id\":").append(i)
                 .append(",\"op\":\"no-such-op\",\"params\":{}}\n");
        }
        ObjectMapper mapper = new ObjectMapper();
        Dispatcher dispatcher = new Dispatcher(new ConnectionManager(), new CursorManager());
        ExecutorService pool = Agent.newRequestPool();
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        try {
            Agent.serve(new BufferedReader(new StringReader(input.toString())),
                        new BufferedOutputStream(sink), mapper, dispatcher, pool);
        } finally {
            pool.shutdown();
            assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
            dispatcher.shutdown();
        }
        String[] lines = sink.toString(StandardCharsets.UTF_8).split("\n");
        assertEquals(requests + 1, lines.length);
        Set<Integer> ids = new HashSet<>();
        for (String line : lines) {
            JsonNode response = mapper.readTree(line);
            assertTrue(ids.add(response.get("id").asInt()),
                       "duplicate response id in: " + line);
        }
        assertTrue(ids.contains(0), "missing ready line");
        for (int i = 1; i <= requests; i++) {
            assertTrue(ids.contains(i), "missing response for request " + i);
        }
    }

    @Test
    void serveFlushesExactlyOncePerLineAndSurfacesWriteFailures() throws Exception {
        // One flush per protocol line: an autoFlush PrintStream underneath
        // would double it, and the standard-output PrintStream would swallow the IOException.
        class FlushCounting extends ByteArrayOutputStream {
            int flushes;
            @Override
            public void flush() throws IOException {
                flushes++;
                super.flush();
            }
        }
        ObjectMapper mapper = new ObjectMapper();
        Dispatcher dispatcher = new Dispatcher(new ConnectionManager(), new CursorManager());
        ExecutorService pool = Agent.newRequestPool();
        FlushCounting sink = new FlushCounting();
        try {
            Agent.serve(new BufferedReader(new StringReader("")), sink,
                        mapper, dispatcher, pool);
            assertEquals(1, sink.flushes, "ready line must flush exactly once");
            byte[] bytes = sink.toByteArray();
            assertEquals('\n', bytes[bytes.length - 1]);

            OutputStream failing = new OutputStream() {
                @Override
                public void write(int b) throws IOException {
                    throw new IOException("stdout gone");
                }
            };
            assertThrows(IOException.class,
                         () -> Agent.serve(new BufferedReader(new StringReader("")),
                                           failing, mapper, dispatcher, pool));
        } finally {
            pool.shutdown();
            assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
            dispatcher.shutdown();
        }
    }

    @Test
    void serveRejectsRequestsBeyondPoolCapacityWithOverloadErrors() throws Exception {
        // Deterministic overload: block every worker on a latch, then submit
        // more requests than pool slots.  The surplus must each get an
        // overload error line while the workers are still held.
        int held = Agent.MAX_CONCURRENT_REQUESTS;
        int surplus = 10;
        CountDownLatch release = new CountDownLatch(1);
        ObjectMapper mapper = new ObjectMapper();
        Dispatcher dispatcher = new Dispatcher(new ConnectionManager(), new CursorManager()) {
            @Override
            public Response dispatch(Request request) {
                try {
                    release.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return Response.error(request.id, "released");
            }
        };
        StringBuilder input = new StringBuilder();
        for (int i = 1; i <= held + surplus; i++) {
            input.append("{\"id\":").append(i)
                 .append(",\"op\":\"held\",\"params\":{}}\n");
        }
        ExecutorService pool = Agent.newRequestPool();
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        try {
            Agent.serve(new BufferedReader(new StringReader(input.toString())),
                        new BufferedOutputStream(sink), mapper, dispatcher, pool);
        } finally {
            release.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
            dispatcher.shutdown();
        }
        int overloaded = 0;
        for (String line : sink.toString(StandardCharsets.UTF_8).split("\n")) {
            JsonNode error = mapper.readTree(line).get("error");
            if (error != null && error.asText().startsWith("Agent overloaded")) {
                overloaded++;
            }
        }
        assertEquals(surplus, overloaded);
    }

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
                  "user":"sa","password":"","auto-commit":false,
                  "validate-after-idle-seconds":300}}
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

            JsonNode savepoint = roundTrip(mapper, dispatcher, """
                {"id":4,"op":"create-savepoint","params":{"conn-id":%d}}
                """.formatted(connId));
            int savepointId = savepoint.path("result").path("savepoint-id").asInt();
            assertTrue(savepointId > 0);
            assertTrue(roundTrip(mapper, dispatcher, """
                {"id":5,"op":"execute","params":{"conn-id":%d,
                  "sql":"INSERT INTO orders VALUES (18, 'batch', NULL)"}}
                """.formatted(connId)).path("ok").asBoolean());
            assertTrue(roundTrip(mapper, dispatcher, """
                {"id":6,"op":"rollback-savepoint","params":{
                  "conn-id":%d,"savepoint-id":%d}}
                """.formatted(connId, savepointId)).path("ok").asBoolean());

            JsonNode query = roundTrip(mapper, dispatcher, """
                {"id":7,"op":"execute","params":{"conn-id":%d,
                  "sql":"SELECT id, note, optional FROM orders ORDER BY id"}}
                """.formatted(connId));
            JsonNode row = query.path("result").path("rows").path(0);
            assertEquals(1, query.path("result").path("rows").size());
            assertEquals(17, row.path(0).asInt());
            assertEquals("中文", row.path(1).asText());
            assertTrue(row.path(2).isNull());
        } finally {
            connections.disconnectAll();
            dispatcher.shutdown();
        }
    }

    @Test
    void jsonProtocolBindsBinaryEnvelopeAsBlobBytes() throws Exception {
        Class.forName("org.h2.Driver");
        ObjectMapper mapper = new ObjectMapper();
        ConnectionManager connections = new ConnectionManager();
        Dispatcher dispatcher = new Dispatcher(connections, new CursorManager());
        String payload = "{\"message\":\"中文\"}";
        byte[] payloadBytes = payload.getBytes(StandardCharsets.UTF_8);
        byte[] rawPayload = new byte[]{0, (byte) 0xff, 0x41, 0x0a};
        String base64 = Base64.getEncoder().encodeToString(payloadBytes);
        String rawBase64 = Base64.getEncoder().encodeToString(rawPayload);
        try {
            JsonNode connect = roundTrip(mapper, dispatcher, """
                {"id":11,"op":"connect","params":{
                  "url":"jdbc:h2:mem:binary_protocol;DB_CLOSE_DELAY=-1",
                  "driver-class":"org.h2.Driver",
                  "user":"sa","password":""}}
                """);
            assertTrue(connect.path("ok").asBoolean());
            int connId = connect.path("result").path("conn-id").asInt();

            assertTrue(roundTrip(mapper, dispatcher, """
                {"id":12,"op":"execute","params":{"conn-id":%d,
                  "sql":"CREATE TABLE documents (id INTEGER PRIMARY KEY, payload BLOB, raw_payload VARBINARY)"}}
                """.formatted(connId)).path("ok").asBoolean());

            JsonNode insert = roundTrip(mapper, dispatcher, """
                {"id":13,"op":"execute-params","params":{"conn-id":%d,
                  "sql":"INSERT INTO documents (id, payload, raw_payload) VALUES (?, ?, ?)",
                  "values":[1,
                    {"__clutch_jdbc_param":"binary","jdbc-type":"BLOB","base64":"%s"},
                    {"__clutch_jdbc_param":"binary","jdbc-type":"RAW","base64":"%s"}]}}
                """.formatted(connId, base64, rawBase64));
            assertTrue(insert.path("ok").asBoolean(), insert.toString());
            assertEquals(1, insert.path("result").path("affected-rows").asInt());

            JsonNode emptyInsert = roundTrip(mapper, dispatcher, """
                {"id":14,"op":"execute-params","params":{"conn-id":%d,
                  "sql":"INSERT INTO documents (id, payload, raw_payload) VALUES (?, ?, ?)",
                  "values":[2,
                    {"__clutch_jdbc_param":"binary","jdbc-type":"BLOB","base64":""},
                    {"__clutch_jdbc_param":"binary","jdbc-type":"RAW","base64":""}]}}
                """.formatted(connId));
            assertTrue(emptyInsert.path("ok").asBoolean(), emptyInsert.toString());
            assertEquals(1, emptyInsert.path("result").path("affected-rows").asInt());

            JsonNode nullInsert = roundTrip(mapper, dispatcher, """
                {"id":15,"op":"execute-params","params":{"conn-id":%d,
                  "sql":"INSERT INTO documents (id, payload, raw_payload) VALUES (?, ?, ?)",
                  "values":[3,
                    {"__clutch_jdbc_param":"binary","jdbc-type":"BLOB","base64":null},
                    {"__clutch_jdbc_param":"binary","jdbc-type":"RAW","base64":null}]}}
                """.formatted(connId));
            assertTrue(nullInsert.path("ok").asBoolean(), nullInsert.toString());
            assertEquals(1, nullInsert.path("result").path("affected-rows").asInt());

            JsonNode query = roundTrip(mapper, dispatcher, """
                {"id":16,"op":"execute","params":{"conn-id":%d,
                  "sql":"SELECT payload FROM documents WHERE id = 1"}}
                """.formatted(connId));
            JsonNode value = query.path("result").path("rows").path(0).path(0);
            assertEquals("blob", value.path("__type").asText());
            assertEquals(payload, value.path("text").asText());
            assertEquals("UTF-8", value.path("encoding").asText());
            try (var stmt = connections.getPrimary(connId).prepareStatement(
                    "SELECT id, payload, raw_payload FROM documents ORDER BY id");
                 var result = stmt.executeQuery()) {
                assertTrue(result.next());
                assertEquals(1, result.getInt(1));
                assertArrayEquals(payloadBytes, result.getBytes(2));
                assertArrayEquals(rawPayload, result.getBytes(3));
                assertTrue(result.next());
                assertEquals(2, result.getInt(1));
                assertArrayEquals(new byte[0], result.getBytes(2));
                assertFalse(result.wasNull());
                assertArrayEquals(new byte[0], result.getBytes(3));
                assertFalse(result.wasNull());
                assertTrue(result.next());
                assertEquals(3, result.getInt(1));
                assertNull(result.getBytes(2));
                assertTrue(result.wasNull());
                assertNull(result.getBytes(3));
                assertTrue(result.wasNull());
                assertFalse(result.next());
            }
        } finally {
            connections.disconnectAll();
            dispatcher.shutdown();
        }
    }

    @Test
    void forceDisconnectBypassesAStuckConnectionLock() throws Exception {
        Class.forName("org.h2.Driver");
        ObjectMapper mapper = new ObjectMapper();
        ConnectionManager connections = new ConnectionManager();
        Dispatcher dispatcher = new Dispatcher(connections, new CursorManager());
        Thread stuck = null;
        try {
            JsonNode connect = roundTrip(mapper, dispatcher, """
                {"id":1,"op":"connect","params":{
                  "url":"jdbc:h2:mem:forcedisc;DB_CLOSE_DELAY=-1",
                  "driver-class":"org.h2.Driver",
                  "user":"sa","password":"","auto-commit":true,
                  "validate-after-idle-seconds":300}}
                """);
            assertTrue(connect.path("ok").asBoolean());
            int connId = connect.path("result").path("conn-id").asInt();

            assertTrue(roundTrip(mapper, dispatcher, """
                {"id":2,"op":"execute","params":{"conn-id":%d,
                  "sql":"CREATE ALIAS IF NOT EXISTS SLEEP FOR 'java.lang.Thread.sleep(long)'"}}
                """.formatted(connId)).path("ok").asBoolean());

            // Occupy the foreground lock the way a stuck JDBC call would.
            java.util.concurrent.CountDownLatch entered =
                new java.util.concurrent.CountDownLatch(1);
            stuck = new Thread(() -> {
                entered.countDown();
                try {
                    roundTrip(mapper, dispatcher, """
                        {"id":3,"op":"execute","params":{"conn-id":%d,
                          "sql":"SELECT SLEEP(3000)"}}
                        """.formatted(connId));
                } catch (Exception ignored) {
                    // The forced close is expected to fail this call.
                }
            }, "stuck-jdbc-call");
            stuck.setDaemon(true);
            stuck.start();
            assertTrue(entered.await(2, java.util.concurrent.TimeUnit.SECONDS));
            Thread.sleep(300); // let the execute reach the driver

            // The ordinary disconnect would block behind the lock; the
            // forced one must return promptly and drop the session.
            long start = System.nanoTime();
            JsonNode forced = roundTrip(mapper, dispatcher, """
                {"id":4,"op":"force-disconnect","params":{"conn-id":%d}}
                """.formatted(connId));
            long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
            assertTrue(forced.path("ok").asBoolean());
            assertTrue(elapsedMillis < 2000,
                "force-disconnect blocked for " + elapsedMillis + "ms");
            assertFalse(connections.hasConnection(connId));

            // Idempotent for connections that are already gone.
            assertTrue(roundTrip(mapper, dispatcher, """
                {"id":5,"op":"force-disconnect","params":{"conn-id":%d}}
                """.formatted(connId)).path("ok").asBoolean());
        } finally {
            // The wedged call sleeps out on its own daemon thread; waiting
            // for it would only bill the test for the prop's duration.
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
