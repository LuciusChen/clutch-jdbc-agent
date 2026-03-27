package clutch.jdbc;

import clutch.jdbc.handler.Dispatcher;
import clutch.jdbc.model.Request;
import clutch.jdbc.model.Response;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

    private static final class ResponseEmitter {
        private final ObjectMapper mapper;
        private final PrintStream out;
        private final Object lock = new Object();

        private ResponseEmitter(ObjectMapper mapper, PrintStream out) {
            this.mapper = mapper;
            this.out = out;
        }

        private void emit(Response response) {
            try {
                String json = mapper.writeValueAsString(response);
                synchronized (lock) {
                    out.println(json);
                    out.flush();
                }
            } catch (Exception e) {
                LOG.log(System.Logger.Level.ERROR,
                        "Failed to write response {0}: {1}",
                        response.id, e.getMessage());
            }
        }
    }

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
        ExecutorService requestPool = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "clutch-jdbc-request");
            t.setDaemon(true);
            return t;
        });

        // stdout must be line-buffered and UTF-8 for the protocol.
        PrintStream out = new PrintStream(
            new BufferedOutputStream(System.out), true, StandardCharsets.UTF_8);
        ResponseEmitter emitter = new ResponseEmitter(mapper, out);

        // Signal readiness to Emacs.
        emitter.emit(Response.ok(0, java.util.Map.of("agent", "clutch-jdbc-agent", "ready", true)));

        // Main loop: one request per line.
        BufferedReader in = new BufferedReader(
            new InputStreamReader(System.in, StandardCharsets.UTF_8));

        String line;
        while ((line = in.readLine()) != null) {
            line = line.strip();
            if (line.isEmpty()) continue;

            try {
                Request req = mapper.readValue(line, Request.class);
                requestPool.submit(() -> {
                    try {
                        emitter.emit(dispatcher.dispatch(req));
                    } catch (Exception e) {
                        LOG.log(System.Logger.Level.ERROR,
                                "Error handling request {0}: {1}",
                                req.id, e.getMessage());
                        emitter.emit(Response.error(req.id, e.getMessage()));
                    }
                });
            } catch (Exception e) {
                LOG.log(System.Logger.Level.ERROR,
                        "Error parsing request: {0}",
                        e.getMessage());
                emitter.emit(Response.error(-1, e.getMessage()));
            }
        }

        // stdin closed — clean up and exit.
        LOG.log(System.Logger.Level.INFO, "stdin closed, shutting down.");
        requestPool.shutdownNow();
        connMgr.disconnectAll();
        dispatcher.shutdown();
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
