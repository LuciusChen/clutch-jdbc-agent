package clutch.jdbc;

import clutch.jdbc.handler.Dispatcher;
import clutch.jdbc.model.Request;
import clutch.jdbc.model.Response;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

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

            Request req = null;
            try {
                req = mapper.readValue(line, Request.class);
                Response resp = dispatcher.dispatch(req);
                out.println(mapper.writeValueAsString(resp));
            } catch (Exception e) {
                int id = (req != null) ? req.id : -1;
                LOG.log(System.Logger.Level.ERROR,
                        "Error handling request {0}: {1}",
                        id, e.getMessage());
                out.println(mapper.writeValueAsString(Response.error(id, e.getMessage())));
            }
        }

        // stdin closed — clean up and exit.
        LOG.log(System.Logger.Level.INFO, "stdin closed, shutting down.");
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
