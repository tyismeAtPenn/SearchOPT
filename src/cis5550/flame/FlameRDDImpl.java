package cis5550.flame;

import java.util.*;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.*;

public class FlameRDDImpl implements FlameRDD {
    private FlameContextImpl context;
    private String tableName;

    public FlameRDDImpl(FlameContextImpl context, String tableName) {
        this.context = context;
        this.tableName = tableName;
    }

    public int count() throws Exception {
        return context.getKVS().count(tableName);
    }

    public void saveAsTable(String tableNameArg) throws Exception {
        context.getKVS().rename(tableName, tableNameArg);
        tableName = tableNameArg;
    }

    public FlameRDD distinct() throws Exception {
        String operation = "/rdd/distinct?inputTable="+tableName;
        String outputTable = context.invokeOperation(operation, null);
        FlameRDD rdd = new FlameRDDImpl(context, outputTable);
        return rdd;
    }

    public void destroy() throws Exception {
        
    }

    public Vector<String> take(int num) throws Exception {
        Iterator<Row> iter = context.getKVS().scan(tableName);
        Vector<String> ret = new Vector<>();
        while (iter.hasNext() && ret.size() < num) {
            Row row = iter.next();
            ret.add(row.get("value"));
        }
        return ret;
    }

    public String fold(String zeroElement, FlamePairRDD.TwoStringsToString lambda) throws Exception {
        String zeroEncoded = java.net.URLEncoder.encode(zeroElement, "UTF-8");
        String operation = "/rdd/fold?inputTable="+tableName+"&zeroElement="+zeroEncoded;
        String outputTable = context.invokeOperation(operation, Serializer.objectToByteArray(lambda));
        return aggregate(outputTable, zeroElement, lambda);
    }

    private String aggregate(String table, String zeroElement, FlamePairRDD.TwoStringsToString lambda) throws Exception {
        KVSClient kvs = context.getKVS();
        String accumulator = new String(zeroElement);
        Iterator<Row> iter = kvs.scan(table, null, null);
        while (iter.hasNext()) {
            Row row = iter.next();
            accumulator = lambda.op(accumulator, row.get("value"));
        }
        return accumulator;
    }
    
    public List<String> collect() throws Exception {
        Iterator<Row> iter = context.getKVS().scan(tableName);
        List<String> ret = new ArrayList<>();
        while (iter.hasNext()) {
            Row row = iter.next();
            ret.add(row.get("value"));
//            System.out.println("get value:"+row.get("value"));
        }
        System.out.println("end collect:");
        return ret;
    }

    public FlameRDD flatMap(StringToIterable lambda) throws Exception {
        String operation = "/rdd/flatMap?inputTable="+tableName;
        String outputTable = context.invokeOperation(operation, Serializer.objectToByteArray(lambda));
        FlameRDD rdd = new FlameRDDImpl(context, outputTable);
        return rdd;
    }

    public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception {
        String operation = "/rdd/flatMapToPair?inputTable="+tableName;
        String outputTable = context.invokeOperation(operation, Serializer.objectToByteArray(lambda));
        FlamePairRDD rdd = new FlamePairRDDImpl(context, outputTable);
        return rdd;
    }

    public FlamePairRDD mapToPair(StringToPair lambda) throws Exception {
        String operation = "/rdd/mapToPair?inputTable="+tableName;
        String outputTable = context.invokeOperation(operation, Serializer.objectToByteArray(lambda));
        FlamePairRDD rdd = new FlamePairRDDImpl(context, outputTable);
        return rdd;
    }

    public FlameRDD intersection(FlameRDD r) throws Exception {
        return null;
    }

    public FlameRDD sample(double f) throws Exception {
        return null;
    }

    public FlamePairRDD groupBy(StringToString lambda) throws Exception {
        return null;
    }

    public FlameRDD filter(StringToBoolean lambda) throws Exception {
        return null;
    }

    public FlameRDD mapPartitions(IteratorToIterator lambda) throws Exception {
        return null;
    }

}
