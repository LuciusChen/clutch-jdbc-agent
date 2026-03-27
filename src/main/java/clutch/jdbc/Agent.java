package clutch.jdbc;

import clutch.jdbc.handler.Dispatcher;
import clutch.jdbc.model.Request;
import clutch.jdbc.model.Response;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Main entry point. Reads one JSON request per line from stdin,
 * writes one JSON response per line to stdout.
 *
 * Startup:
 *   java -jar clutch-jdbc-agent.jar [drivers-dir]
 *
 * drivers-dir defaults to "./drivers" relative to the jar location.
 *
 * stderr is used exclusively for logging — never for protocol messages.
 */
public class Agent {

    private static final System.Logger LOG = System.getLogger(Agent.class.getName());
    static final int MAX_CONCURRENT_REQUESTS = 48;
    private static final String REQUEST_OVERLOADED_ERROR =
        "Agent overloaded: too many concurrent requests";

    /** Start the agent: load drivers, emit ready signal, then loop on stdin. */
    public static void main(String[] args) throws Exception {
        // Determine drivers directory.
        File driversDir = args.length > 0
            ? new File(args[0])
            : resolveDefaultDriversDir();

        LOG.log(System.Logger.Level.INFO, "clutch-jdbc-agent starting. Drivers dir: {0}",
                driversDir.getAbsolutePath());

        // Load external JDBC drivers.
        var loaded = DriverLoader.loadDrivers(driversDir);
        LOG.log(System.Logger.Level.INFO, "Loaded {0} driver(s): {1}", loaded.size(), loaded);

        // Infrastructure.
        ObjectMapper mapper   = new ObjectMapper();
        ConnectionManager connMgr   = new ConnectionManager();
        CursorManager cursorMgr     = new CursorManager();
        Dispatcher dispatcher = new Dispatcher(connMgr, cursorMgr);
        ExecutorService requestPool = newRequestPool();

        // stdout must be line-buffered and UTF-8 for the protocol.
        PrintStream out = new PrintStream(
            new BufferedOutputStream(System.out), true, StandardCharsets.UTF_8);

        // Signal readiness to Emacs.
        out.println(mapper.writeValueAsString(
            Response.ok(0, java.util.Map.of("agent", "clutch-jdbc-agent", "ready", true))));

        // Main loop: one request per line.
        BufferedReader in = new BufferedReader(
            new InputStreamReader(System.in, StandardCharsets.UTF_8));

        String line;
        while ((line = in.readLine()) != null) {
            line = line.strip();
            if (line.isEmpty()) continue;

            Request req;
            try {
                req = mapper.readValue(line, Request.class);
            } catch (Exception e) {
                LOG.log(System.Logger.Level.ERROR,
                        "Error parsing request line: {0}",
                        e.getMessage());
                synchronized (out) {
                    out.println(mapper.writeValueAsString(Response.error(-1, e.getMessage())));
                }
                continue;
            }

            try {
                requestPool.submit(() -> handleRequest(dispatcher, mapper, out, req));
            } catch (RejectedExecutionException e) {
                LOG.log(System.Logger.Level.WARNING,
                        "Rejecting request {0}: request pool saturated",
                        req.id);
                synchronized (out) {
                    out.println(mapper.writeValueAsString(
                        Response.error(req.id, REQUEST_OVERLOADED_ERROR)));
                }
            } catch (Exception e) {
                LOG.log(System.Logger.Level.ERROR,
                        "Error submitting request {0}: {1}",
                        req.id,
                        e.getMessage());
                synchronized (out) {
                    out.println(mapper.writeValueAsString(Response.error(req.id, e.getMessage())));
                }
            }
        }

        // stdin closed — clean up and exit.
        LOG.log(System.Logger.Level.INFO, "stdin closed, shutting down.");
        requestPool.shutdownNow();
        connMgr.disconnectAll();
        dispatcher.shutdown();
    }

    private static ExecutorService newRequestPool() {
        ThreadPoolExecutor pool = new ThreadPoolExecutor(
            MAX_CONCURRENT_REQUESTS,
            MAX_CONCURRENT_REQUESTS,
            60L,
            TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            r -> {
                Thread t = new Thread(r, "clutch-jdbc-request");
                t.setDaemon(true);
                return t;
            });
        pool.allowCoreThreadTimeOut(true);
        return pool;
    }

    private static void handleRequest(Dispatcher dispatcher, ObjectMapper mapper,
                                      PrintStream out, Request req) {
        try {
            Response resp = dispatcher.dispatch(req);
            synchronized (out) {
                out.println(mapper.writeValueAsString(resp));
            }
        } catch (Exception e) {
            LOG.log(System.Logger.Level.ERROR,
                    "Error handling request {0}: {1}",
                    req.id, e.getMessage());
            try {
                synchronized (out) {
                    out.println(mapper.writeValueAsString(Response.error(req.id, e.getMessage())));
                }
            } catch (IOException ioException) {
                LOG.log(System.Logger.Level.ERROR,
                        "Error writing error response for request {0}: {1}",
                        req.id, ioException.getMessage());
            }
        }
    }

    /**
     * Resolve the default drivers/ directory relative to the running jar.
     * Falls back to ./drivers if the jar path cannot be determined (e.g. during dev).
     */
    private static File resolveDefaultDriversDir() {
        try {
            Path jar = Path.of(Agent.class.getProtectionDomain()
                                         .getCodeSource()
                                         .getLocation()
                                         .toURI());
            return jar.getParent().resolve("drivers").toFile();
        } catch (Exception e) {
            return new File("drivers");
        }
    }
}
