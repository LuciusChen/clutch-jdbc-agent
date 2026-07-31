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
 * stderr carries agent logs and quarantined third-party console output,
 * never protocol messages.
 */
public class Agent {

    private static final System.Logger LOG = System.getLogger(Agent.class.getName());
    static final int MAX_CONCURRENT_REQUESTS = 48;
    private static final String REQUEST_OVERLOADED_ERROR =
        "Agent overloaded: too many concurrent requests";

    /** Start the agent: load drivers, emit ready signal, then loop on stdin. */
    public static void main(String[] args) throws Exception {
        // Keep an independent handle to the real stdout for protocol writes,
        // then quarantine Java-level stdout before any third-party driver code
        // can load.  Some drivers print login/status text unconditionally.
        OutputStream out = new BufferedOutputStream(
            new FileOutputStream(FileDescriptor.out));
        System.setOut(new PrintStream(System.err, true, StandardCharsets.UTF_8));

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

        // The dedicated stdout handle is flushed exactly once per protocol
        // line by writeLine and surfaces IOExceptions directly.
        BufferedReader in = new BufferedReader(
            new InputStreamReader(System.in, StandardCharsets.UTF_8));

        serve(in, out, mapper, dispatcher, requestPool);

        // stdin closed — clean up and exit.
        LOG.log(System.Logger.Level.INFO, "stdin closed, shutting down.");
        requestPool.shutdownNow();
        connMgr.disconnectAll();
        dispatcher.shutdown();
    }

    /**
     * Serve the line protocol on IN/OUT until IN is exhausted: emit the
     * ready signal, then dispatch one JSON request per line.
     * Package-private so tests can drive the real framing and locking.
     */
    static void serve(BufferedReader in, OutputStream out, ObjectMapper mapper,
                      Dispatcher dispatcher, ExecutorService requestPool)
            throws IOException {
        // Signal readiness to Emacs.
        writeLine(mapper, out,
            Response.ok(0, java.util.Map.of("agent", "clutch-jdbc-agent", "ready", true)));

        // Main loop: one request per line.
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
                writeLine(mapper, out, Response.error(-1, e.getMessage()));
                continue;
            }

            try {
                requestPool.submit(() -> handleRequest(dispatcher, mapper, out, req));
            } catch (RejectedExecutionException e) {
                LOG.log(System.Logger.Level.WARNING,
                        "Rejecting request {0}: request pool saturated",
                        req.id);
                writeLine(mapper, out, Response.error(req.id, REQUEST_OVERLOADED_ERROR));
            }
        }
    }

    static ExecutorService newRequestPool() {
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
                                      OutputStream out, Request req) {
        try {
            Response resp = dispatcher.dispatch(req);
            writeLine(mapper, out, resp);
        } catch (Exception e) {
            LOG.log(System.Logger.Level.ERROR,
                    "Error handling request {0}: {1}",
                    req.id, e.getMessage());
            try {
                writeLine(mapper, out, Response.error(req.id, e.getMessage()));
            } catch (IOException ioException) {
                LOG.log(System.Logger.Level.ERROR,
                        "Error writing error response for request {0}: {1}",
                        req.id, ioException.getMessage());
            }
        }
    }

    /**
     * Serialize RESP straight to UTF-8 bytes and write it as one protocol line.
     * Skipping the intermediate UTF-16 String halves the copies on the
     * response hot path; the synchronized block keeps each line atomic and
     * the single explicit flush pushes payload and newline together.
     */
    private static void writeLine(ObjectMapper mapper, OutputStream out, Response resp)
            throws IOException {
        byte[] payload = mapper.writeValueAsBytes(resp);
        synchronized (out) {
            out.write(payload, 0, payload.length);
            out.write('\n');
            out.flush();
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
