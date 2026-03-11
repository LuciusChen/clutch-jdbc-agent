package clutch.jdbc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages open cursors (Statement + ResultSet pairs).
 *
 * Lifecycle:
 *   execute → cursorId  (Statement + ResultSet held open)
 *   fetch   → rows      (advance ResultSet by fetchSize rows)
 *   close   → release   (close Statement + ResultSet)
 *
 * Cursors are also closed on disconnect (via closeForConnection).
 */
public class CursorManager {

    private static final System.Logger LOG = System.getLogger(CursorManager.class.getName());

    private record Cursor(int connId, Statement stmt, ResultSet rs,
                          List<String> columnNames, List<String> columnTypes) {}

    private final AtomicInteger nextId = new AtomicInteger(1);
    private final Map<Integer, Cursor> cursors = new ConcurrentHashMap<>();

    /**
     * Register a newly-opened Statement + ResultSet and return a cursorId.
     */
    public int register(int connId, Statement stmt, ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int colCount = meta.getColumnCount();
        List<String> names = new ArrayList<>(colCount);
        List<String> types = new ArrayList<>(colCount);
        for (int i = 1; i <= colCount; i++) {
            names.add(meta.getColumnLabel(i));
            types.add(meta.getColumnTypeName(i));
        }
        int id = nextId.getAndIncrement();
        cursors.put(id, new Cursor(connId, stmt, rs, names, types));
        return id;
    }

    /**
     * Fetch up to fetchSize rows from cursor.
     * Returns null columns list when the cursor is exhausted (done=true).
     */
    public FetchResult fetch(int cursorId, int fetchSize) throws SQLException {
        Cursor c = cursors.get(cursorId);
        if (c == null) throw new SQLException("Unknown cursor id: " + cursorId);

        List<List<Object>> rows = new ArrayList<>(fetchSize);
        boolean done = false;
        int colCount = c.columnNames().size();

        for (int i = 0; i < fetchSize; i++) {
            if (!c.rs().next()) { done = true; break; }
            List<Object> row = new ArrayList<>(colCount);
            for (int col = 1; col <= colCount; col++) {
                row.add(TypeConverter.convert(c.rs(), col));
            }
            rows.add(row);
        }

        if (done) close(cursorId);
        return new FetchResult(c.columnNames(), c.columnTypes(), rows, done);
    }

    /** Close and remove the cursor for {@code cursorId}. No-op if already closed. */
    public void close(int cursorId) {
        Cursor c = cursors.remove(cursorId);
        if (c == null) return;
        try { c.rs().close();   } catch (Exception ignored) {}
        try { c.stmt().close(); } catch (Exception ignored) {}
    }

    /** Close all cursors belonging to a connection (called on disconnect). */
    public void closeForConnection(int connId) {
        cursors.entrySet().removeIf(e -> {
            if (e.getValue().connId() == connId) {
                close(e.getKey());
                return true;
            }
            return false;
        });
    }

    public record FetchResult(List<String> columns, List<String> types,
                              List<List<Object>> rows, boolean done) {}
}
