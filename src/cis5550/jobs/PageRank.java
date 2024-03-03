package cis5550.jobs;

import java.util.*;

import cis5550.flame.*;
import cis5550.flame.FlamePairRDD.PairToPairIterable;
import cis5550.kvs.*;
import cis5550.jobs.Crawler;
import cis5550.tools.Hasher;

import org.jsoup.Jsoup;

public class PageRank {
    private static double decay = 0.85;

    public static void run(FlameContext context, String[] args) throws Exception {
        if (args.length < 1) {
            context.output("Convergence threshold not exists!");
            return;
        }
        double threshold = Double.parseDouble(args[0]);

        // HW9 Step #3: Load the data (PageRank)
        FlamePairRDD stateTable = context.fromTable("pt-crawl", (row) -> {
            String page = row.get("page") != null ? row.get("page") : ""; 
            String cleanPage = Jsoup.parse(page).body().text();
            return row.get("url")+","+cleanPage;
        }).mapToPair((str) -> {
            String[] pair = str.split(",", 2);
            String url = pair[0];
            String page = pair[1];
            String key = Hasher.hash(url);
            
            // List<String> list = Crawler.extractURLs(url, page.getBytes());
            List<String> list = extractAndNormalize(url, page);
            String urls = list.stream().map((s)->Hasher.hash(s)).reduce("", (s1, s2) -> s1+","+s2);
            String value = "1.0,1.0" + urls;
            return new FlamePair(key, value);
        });

        int count = 0;
        while (true) {
            // HW9 Step #4,5: Compute and aggregate the transfers
            FlamePairRDD transferTable = stateTable.flatMapToPair((pair) -> {
                // Parse attributes
                String url = pair._1();
                String valueString = pair._2();
                String[] values = valueString.split(",", 3);
                double currRank = Double.parseDouble(values[0]);
                double prevRank = Double.parseDouble(values[1]);
                //
                String linkString = values.length == 3 ? values[2] : null;
                String[] links = linkString != null ? linkString.split(",") : new String[0];
                int n = links.length;
                List<FlamePair> vs = new ArrayList<>();
                vs.add(new FlamePair(url, "0.0"));
                for (String link : links) {
                    double v = decay * currRank / n;
                    vs.add(new FlamePair(link, String.valueOf(v)));
                    // System.out.println(link + ", " + v);
                }
                return vs;
            }).foldByKey("0.0", (str1, str2) -> {
                double v1 = Double.parseDouble(str1);
                double v2 = Double.parseDouble(str2);
                return String.valueOf(v1 + v2);
            });
        
            // HW9 Step #6: Update the state table
            stateTable = stateTable.join(transferTable).flatMapToPair((pair) -> {
                String url = pair._1();
                String joinString = pair._2();

                int split = joinString.lastIndexOf(",");
                String valueString = joinString.substring(0, split);
                String[] values = valueString.split(",", 3);
                double currRank = Double.parseDouble(values[0]);
                double prevRank = Double.parseDouble(values[1]);
                String linkString = values.length == 3 ? ","+values[2] : "";

                double v = Double.parseDouble(joinString.substring(split+1));
                prevRank = currRank;
                currRank = v + 0.15;
                String value = currRank+","+prevRank+linkString;
                return Arrays.asList(new FlamePair(url, value));
            });

            // // TODO: For debug purpose
            // break;

            // HW9 Step #7: Check convergence
            String diffString = stateTable.flatMap((pair) -> {
                String valueString = pair._2();
                String[] values = valueString.split(",", 3);
                double currRank = Double.parseDouble(values[0]);
                double prevRank = Double.parseDouble(values[1]);
                double absDiff = Math.abs(currRank - prevRank);
                return Arrays.asList(String.valueOf(absDiff));
            }).fold("0.0", (str1, str2) -> {
                double val1 = Double.parseDouble(str1);
                double val2 = Double.parseDouble(str2);
                return String.valueOf(Math.max(val1, val2));
            });
            double maxDiff = Double.parseDouble(diffString);
            if ((++count) % 100 == 0) {
                System.out.println(count + ": " + maxDiff);
            }
            if (maxDiff < threshold) {
                break;
            }
        }

        // KVSClient kvs = new KVSClient(((FlameContextImpl)context).temp);
        String kvsAddress = ((FlameContextImpl)context).kvsAddress;
        System.out.println("Store data to KVS");
        // HW9 Step #8: Save the results
        stateTable.flatMapToPair((pair) -> {
            String url = pair._1();
            String[] values = pair._2().split(",", 3);
            String rank = values[0];
            // context.getKVS().put("pt-pageranks", url, "rank", rank);
            KVSClient kvs = new KVSClient(kvsAddress);
            kvs.put("pt-pageranks", url, "rank", rank);
            return new ArrayList<>();
        });

        context.output("OK");
    }

    private static List<String> extractAndNormalize(String seed, String page) {
        URLManager um = new URLManager(seed);
        List<String> rawURLs = Crawler.extractURLs(page);
        List<String> cleanedURLs = new ArrayList<>();
        for (String rawURL : rawURLs) {
            String normalized = um.processCrawledURL(rawURL);
            if (normalized == null) {
                continue;
            }
            cleanedURLs.add(normalized);
        }
        return cleanedURLs;
    }
}
