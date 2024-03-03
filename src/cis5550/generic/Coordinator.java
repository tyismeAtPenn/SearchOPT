package cis5550.generic;

import cis5550.utils.WorkerEntry;
import static cis5550.webserver.Server.*;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This coordinator class oversees the workers and their status in the system.
 *
 * @author Lang Qin
 */

public class Coordinator {

    public static final int TIMEOUT = 15000;

    private static final Map<String, WorkerEntry> WORKERS = new ConcurrentHashMap<>();

    /**
     * Get current list of workers.
     *
     * @return List<String> the list of workers as "[workerID],[ip]:[port]" strings
     */
    public static List<String> getWorkers() {
        return WORKERS.values().stream()
                .filter(worker -> System.currentTimeMillis() - worker.getLastPing() < TIMEOUT)
                .map(worker -> worker.getId() + "," + worker.getIp() + ":" + worker.getPort())
                .collect(java.util.stream.Collectors.toList());
    }

    /**
     * Get an HTML page with a table that contains an entry for each active worker
     * and lists its ID, IP, and port.
     * </>
     * Each entry should have a hyperlink to "http://[IP]:[port]/", where [ip] and [port]
     * are the IP address and port of the worker.
     *
     * @return String the HTML page as a string
     */
    public static String workerTable() {
        StringBuilder sb = new StringBuilder();
        sb.append("<html><body><table border=\"1\"><tr><th>ID</th><th>IP</th><th>Port</th><th>Link</th></tr>");
        for (WorkerEntry worker : WORKERS.values()) {
            if (System.currentTimeMillis() - worker.getLastPing() > TIMEOUT) {
                continue;
            }
            sb.append("<tr><td>").append(worker.getId())
                    .append("</td><td>").append(worker.getIp())
                    .append("</td><td>").append(worker.getPort())
                    .append("</td><td><a href=\"http://").append(worker.getIp()).append(":").append(worker.getPort()).append("/\">")
                    .append(worker.getIp()).append(":").append(worker.getPort()).append("/")
                    .append("</a></td></tr>");
        }
        sb.append("</table></body></html>");
        return sb.toString();
    }

    /**
     * Add routes to the Spark web server.
     * </>
     * GET /ping?id=x&port=y - add an entry for the worker with ID x, IP address req.ip(), and port y;
     *     if an entry for x already exists, update the IP address and port and the timestamp of the last ping.
     *     Return a 400 error if the request does not contain the id and/or port parameters.
     * </>
     * GET /workers - return k + 1 lines of text separated by LFs, where k is the number of workers in the system:
     *     the first line contains k, and each of the next k lines contains an entry for a worker in the format
     *     "[id], [ip]:[port]".
     * </>
     * GET / - return an HTML page with a table that contains an entry for each active worker described in @workerTable.
     * </>
     * A worker does not make a \ping request within 15 seconds should be considered failed.
     */
    public static void registerRoutes() {
        get("/ping", (request, response) -> {
            String id = request.queryParams("id");
            String ip = request.ip();
            if (ip == null) {
                response.status(400, "Bad Request");
                return "Bad Request";
            }
            int port = Integer.parseInt(request.queryParams("port"));
            long lastPing = System.currentTimeMillis();
            WorkerEntry worker = new WorkerEntry(id, ip, port, lastPing);
            WORKERS.put(id, worker);
            response.type("text/plain");
            return "OK";
        });

        get("/workers", (request, response) -> {
            List<String> activeWorkers = getWorkers();
            StringBuilder sb = new StringBuilder();
            sb.append(activeWorkers.size()).append("\n");
            for (String worker : activeWorkers) {
                sb.append(worker).append("\n");
            }
            return sb.toString();
        });
    }
}
