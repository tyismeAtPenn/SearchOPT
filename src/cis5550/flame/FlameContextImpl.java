package cis5550.flame;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Vector;

import cis5550.kvs.KVSClient;
import cis5550.tools.*;
import cis5550.tools.Partitioner.Partition;

public class FlameContextImpl implements FlameContext {
    private KVSClient kvs;
    public String kvsAddress;
    protected String outputString = "";

    public FlameContextImpl(KVSClient kvs, String kvsAddress, String jarName) {
        this.kvs = kvs;
        this.kvsAddress = kvsAddress;
    }

    public KVSClient getKVS() {
        // Should be one kvs per /submit from Coordinator?
        return kvs;
    }

    public void output(String s) {
        outputString += s;
    }

    public String nameTable() {
        long jobID = System.currentTimeMillis();
        String jarName = "job-"+Coordinator.nextJobID+".jar";
        String tableName = jobID + "_" + jarName;
        return tableName;
    }

    public FlameRDD parallelize(List<String> list) throws Exception {
        String tableName = nameTable();
        for (int i = 0; i < list.size(); ++i) {
            String value = list.get(i);
            getKVS().put(tableName, Hasher.hash(String.valueOf(i+1)), "value", value.getBytes());
        }
        FlameRDDImpl rdd = new FlameRDDImpl(this, tableName);
        return rdd;
    }

    public FlameRDD fromTable(String tableName, RowToString lambda) throws Exception {
        String operation = "/rdd/fromTable?inputTable="+tableName;
        String outputTable = invokeOperation(operation, Serializer.objectToByteArray(lambda));
        FlameRDD rdd = new FlameRDDImpl(this, outputTable);
        return rdd;
    }

    public void setConcurrencyLevel(int keyRangesPerWorker) {
        
    }

    // HW6: Step #4; Invoked by FlameRDDImpl / FlamePairRDDImpl
    public String invokeOperation(String operation, byte[] lambda) throws IOException {
        // e.g. : operation = "/rdd/flatMap"
        String outputTable = nameTable();
        // Use Partitioner class to find a key assignment to Flame workers
        Partitioner p = new Partitioner();
        // Invoke addKVSWorker for each KVS worker
        int numWorkers = getKVS().numWorkers();
        for (int i = 0; i < numWorkers; ++i) {
            String address = getKVS().getWorkerAddress(i);
            String id = getKVS().getWorkerID(i);
            String nextId = i < numWorkers-1 ? getKVS().getWorkerID(i+1) : null;
            p.addKVSWorker(address, id, nextId);
        }
        // Exception for last worker
        String lastAddress = getKVS().getWorkerAddress(numWorkers-1);
        String firstId = getKVS().getWorkerID(0);
        p.addKVSWorker(lastAddress, null, firstId);
        // Call addFlameWorker for each Flame worker
        for (String entry : Coordinator.getWorkers()) {
            p.addFlameWorker(entry.split(",")[1]);
        }
        Vector<Partition> parts = p.assignPartitions();
        Thread[] threads = new Thread[parts.size()];
        String[] results = new String[parts.size()];
        int[] statusCodes = new int[parts.size()];
        for (int i = 0; i < parts.size(); ++i) {
            Partition part = parts.get(i);
            String flameWorker = part.assignedFlameWorker;
            StringBuilder sb = new StringBuilder("http://"+flameWorker+operation);
            sb.append("&outputTable="+outputTable);
            sb.append("&kvsCoordinator="+getKVS().getCoordinator());
            if (part.fromKey != null) {
                sb.append("&fromKey="+part.fromKey);
            }
            if (part.toKeyExclusive != null) {
                sb.append("&toKeyExclusive="+part.toKeyExclusive);
            }
            final String url = sb.toString();
            final int j = i;
            threads[i] = new Thread(() -> {
                try {
                    HTTP.Response response = HTTP.doRequest("POST", url, lambda);
                    results[j] = new String(response.body());
                    statusCodes[j] = response.statusCode();
                } catch (IOException e) {
                    results[j] = "Exception: "+e;
                    statusCodes[j] = 503;
                    e.printStackTrace();
                }
            });
            threads[i].start();
        }
        // Wait threads
        for (int i = 0; i < threads.length; ++i) {
            try {
                threads[i].join();
                // Check error
                if (statusCodes[i] != 200) {
                    // TODO: Report to caller
                }
            } catch (InterruptedException ie) {

            }
        }
        return outputTable;
    }
}
