package cis5550.kvs;

import static cis5550.webserver.Server.*;

/**
 * This coordinator class oversees the workers and their status in the system.
 * This class extends the generic coordinator class.
 *
 * @author Lang Qin
 */
public class Coordinator extends cis5550.generic.Coordinator {

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: Coordinator <port>");
            System.exit(1);
        }

        int port = Integer.parseInt(args[0]);
        port(port);

        registerRoutes();

        get("/", (request, response) -> workerTable());
    }

}
