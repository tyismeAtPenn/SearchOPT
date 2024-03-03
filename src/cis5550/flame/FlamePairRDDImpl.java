package cis5550.flame;

import java.util.*;

import cis5550.kvs.Row;
import cis5550.tools.Serializer;

public class FlamePairRDDImpl implements FlamePairRDD {
    private FlameContextImpl context;
    private String tableName;

    public FlamePairRDDImpl(FlameContextImpl context, String tableName) {
        this.context = context;
        this.tableName = tableName;
    }
    
    public List<FlamePair> collect() throws Exception {
        Iterator<Row> iter = context.getKVS().scan(tableName);
        List<FlamePair> ret = new ArrayList<>();
        while (iter.hasNext()) {
            Row row = iter.next();
            for (String col : row.columns()) {
                ret.add(new FlamePair(row.key(), row.get(col)));
            }
        }
        return ret;
    }

    public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception {
        String zeroEncoded = java.net.URLEncoder.encode(zeroElement, "UTF-8");
        String operation = "/pairRdd/foldByKey?inputTable="+tableName+"&zeroElement="+zeroEncoded;
        String outputTable = context.invokeOperation(operation, Serializer.objectToByteArray(lambda));
        FlamePairRDD rdd = new FlamePairRDDImpl(context, outputTable);
        return rdd;
    }

    public void saveAsTable(String tableNameArg) throws Exception {
        context.getKVS().rename(tableName, tableNameArg);
        tableName = tableNameArg;
    }

    public FlameRDD flatMap(PairToStringIterable lambda) throws Exception {
        String operation = "/pairRdd/flatMap?inputTable="+tableName;
        String outputTable = context.invokeOperation(operation, Serializer.objectToByteArray(lambda));
        FlameRDD rdd = new FlameRDDImpl(context, outputTable);
        return rdd;
    }

    public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception {
        String operation = "/pairRdd/flatMapToPair?inputTable="+tableName;
        String outputTable = context.invokeOperation(operation, Serializer.objectToByteArray(lambda));
        FlamePairRDD rdd = new FlamePairRDDImpl(context, outputTable);
        return rdd;
    }

    public void destroy() throws Exception {
        
    }

    public FlamePairRDD join(FlamePairRDD other) throws Exception {
        String otherTable = ((FlamePairRDDImpl) other).tableName;
        String operation = "/pairRdd/join?inputTable="+tableName+"&"+"otherTable="+otherTable;
        String outputTable = context.invokeOperation(operation, null);
        FlamePairRDD rdd = new FlamePairRDDImpl(context, outputTable);
        return rdd;
    }

    public FlamePairRDD cogroup(FlamePairRDD other) throws Exception {
        return null;
    }

}
