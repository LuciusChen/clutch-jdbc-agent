package clutch.jdbc;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CursorManagerTest {

    @Test
    void registerAndFetchReturnsAllRowsThenDone() throws SQLException {
        CursorManager mgr = new CursorManager();
        int cursorId = mgr.register(1, mockStatement(),
            mockResultSet(List.of("name", "age"), List.of("VARCHAR", "INTEGER"),
                rows(new Object[]{"alice", 30}, new Object[]{"bob", 25})));

        CursorManager.FetchResult result = mgr.fetch(cursorId, 100);
        assertTrue(result.done());
        assertEquals(List.of("name", "age"), result.columns());
        assertEquals(List.of("VARCHAR", "INTEGER"), result.types());
        assertEquals(2, result.rows().size());
        assertEquals("alice", result.rows().get(0).get(0));
        assertEquals(25, result.rows().get(1).get(1));
    }

    @Test
    void fetchInBatchesReturnsDoneFalseUntilExhausted() throws SQLException {
        CursorManager mgr = new CursorManager();
        int cursorId = mgr.register(1, mockStatement(),
            mockResultSet(List.of("x"), List.of("INT"),
                rows(new Object[]{1}, new Object[]{2}, new Object[]{3})));

        CursorManager.FetchResult batch1 = mgr.fetch(cursorId, 2);
        assertFalse(batch1.done());
        assertEquals(2, batch1.rows().size());
        assertEquals(1, batch1.rows().get(0).get(0));
        assertEquals(2, batch1.rows().get(1).get(0));

        CursorManager.FetchResult batch2 = mgr.fetch(cursorId, 2);
        assertTrue(batch2.done());
        assertEquals(1, batch2.rows().size());
        assertEquals(3, batch2.rows().get(0).get(0));
    }

    @Test
    void fetchUnknownCursorThrows() {
        CursorManager mgr = new CursorManager();
        SQLException err = assertThrows(SQLException.class, () -> mgr.fetch(999, 10));
        assertTrue(err.getMessage().contains("999"));
    }

    @Test
    void closeIsIdempotent() throws SQLException {
        CursorManager mgr = new CursorManager();
        boolean[] closed = {false};
        int cursorId = mgr.register(1, mockStatement(),
            mockResultSet(List.of("x"), List.of("INT"), List.of()));

        mgr.close(cursorId);
        mgr.close(cursorId); // second close is a no-op
        // fetch after close should throw
        assertThrows(SQLException.class, () -> mgr.fetch(cursorId, 1));
    }

    @Test
    void closeForConnectionClosesOnlyMatchingCursors() throws SQLException {
        CursorManager mgr = new CursorManager();
        int c1 = mgr.register(10, mockStatement(),
            mockResultSet(List.of("a"), List.of("INT"), rows(new Object[]{1})));
        int c2 = mgr.register(20, mockStatement(),
            mockResultSet(List.of("b"), List.of("INT"), rows(new Object[]{2})));

        mgr.closeForConnection(10);

        // cursor for conn 10 should be gone
        assertThrows(SQLException.class, () -> mgr.fetch(c1, 1));
        // cursor for conn 20 should still work
        CursorManager.FetchResult result = mgr.fetch(c2, 10);
        assertNotNull(result);
        assertEquals(1, result.rows().size());
        assertEquals(2, result.rows().get(0).get(0));
    }

    @Test
    void fetchAutoClosesWhenDone() throws SQLException {
        CursorManager mgr = new CursorManager();
        int cursorId = mgr.register(1, mockStatement(),
            mockResultSet(List.of("x"), List.of("INT"), rows(new Object[]{1})));

        CursorManager.FetchResult result = mgr.fetch(cursorId, 10);
        assertTrue(result.done());

        // cursor should be auto-closed after done
        assertThrows(SQLException.class, () -> mgr.fetch(cursorId, 1));
    }

    @Test
    void registerAssignsDistinctCursorIds() throws SQLException {
        CursorManager mgr = new CursorManager();
        int id1 = mgr.register(1, mockStatement(),
            mockResultSet(List.of("a"), List.of("INT"), List.of()));
        int id2 = mgr.register(1, mockStatement(),
            mockResultSet(List.of("b"), List.of("INT"), List.of()));
        assertTrue(id1 != id2, "cursor ids should be unique");
    }

    // --- helpers ---

    private static List<Object[]> rows(Object[]... rows) {
        return List.of(rows);
    }

    private static Statement mockStatement() {
        return (Statement) Proxy.newProxyInstance(
            CursorManagerTest.class.getClassLoader(),
            new Class<?>[]{Statement.class},
            (_proxy, method, _args) -> switch (method.getName()) {
                case "close" -> null;
                case "unwrap" -> null;
                case "isWrapperFor" -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });
    }

    private static ResultSet mockResultSet(List<String> colNames, List<String> colTypes,
                                           List<Object[]> rows) {
        AtomicInteger rowIndex = new AtomicInteger(-1);
        ResultSetMetaData meta = (ResultSetMetaData) Proxy.newProxyInstance(
            CursorManagerTest.class.getClassLoader(),
            new Class<?>[]{ResultSetMetaData.class},
            (_proxy, method, args) -> switch (method.getName()) {
                case "getColumnCount" -> colNames.size();
                case "getColumnLabel" -> colNames.get((int) args[0] - 1);
                case "getColumnTypeName" -> colTypes.get((int) args[0] - 1);
                default -> throw new UnsupportedOperationException(method.getName());
            });

        return (ResultSet) Proxy.newProxyInstance(
            CursorManagerTest.class.getClassLoader(),
            new Class<?>[]{ResultSet.class},
            (_proxy, method, args) -> switch (method.getName()) {
                case "getMetaData" -> meta;
                case "next" -> rowIndex.incrementAndGet() < rows.size();
                case "getObject" -> {
                    int idx = rowIndex.get();
                    int col = (int) args[0];
                    yield (idx >= 0 && idx < rows.size()) ? rows.get(idx)[col - 1] : null;
                }
                case "wasNull" -> {
                    int idx = rowIndex.get();
                    yield false;
                }
                case "getString" -> {
                    int idx = rowIndex.get();
                    int col = (int) args[0];
                    Object v = (idx >= 0 && idx < rows.size()) ? rows.get(idx)[col - 1] : null;
                    yield v == null ? null : v.toString();
                }
                case "close" -> null;
                case "unwrap" -> null;
                case "isWrapperFor" -> false;
                default -> throw new UnsupportedOperationException(method.getName());
            });
    }
}
