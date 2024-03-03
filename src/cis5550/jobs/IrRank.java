package cis5550.jobs;


import java.nio.file.*;
import java.util.*;
import java.util.stream.*;
import java.io.IOException;

import cis5550.flame.*;
import cis5550.kvs.*;
import cis5550.external.PorterStemmer;
import cis5550.tools.Hasher;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;


public class IrRank {
    final static int IN_MEM_WORD = 6000;

    public static void run(FlameContext context, String[] args) throws Exception {
        // Load stopwords
        Set<String> stopwords = loadStopwords();
        // Load dictionary
        Set<String> dictionary = loadDictionary();
        // HW9 Step #1: Load the data (indexer)
        FlamePairRDD pairRdd = context.fromTable("pt-crawl", (row) -> {
            String page = row.get("page") != null ? row.get("page") : ""; 
            String cleanPage = Jsoup.parse(page).body().text();
            return row.get("url")+","+cleanPage;
        }).mapToPair((str) -> {
            String[] pair = str.split(",", 2);
            return new FlamePair(pair[0], pair[1]);
        });

        String kvsAddress = ((FlameContextImpl)context).kvsAddress;

        // Note that we want to use a new table each time and only want to delete once

        // Compute tf factor
        pairRdd.flatMapToPair((pair) -> {
            KVSClient kvs = new KVSClient(kvsAddress);
            String url = pair._1();
            String page = pair._2();
            Set<String> words = pageToWords(page);
            // words.stream().forEach((s) -> {System.out.println(String.valueOf(s.equals("")));});
            Stream<String> wordStream = words.stream().map((s) -> {
                PorterStemmer stemmer = new PorterStemmer();
                stemmer.add(s.toCharArray(), s.length());
                stemmer.stem();
                return stemmer.toString();}
            ).filter((s) -> !filter(stopwords, dictionary, s));

            Map<String, Integer> wordUrlCnt = new HashMap<>();
            Map<String, Long> countMap = wordStream.collect(Collectors.groupingBy(s->s, Collectors.counting()));
            Optional<Long> maxCount = countMap.values().stream().max(Long::compare);
            if (maxCount.isEmpty() || maxCount.get() == 0) {
                return new ArrayList<>();
            }

            for (String word : countMap.keySet()) {
                double count = countMap.get(word);
                if(kvs.existsRow("pt-tfs",word))
                    kvs.put("pt-tfs", word, Hasher.hash(url), normalizeTf(count, maxCount.get()));
                else
                {
                    kvs.put("tfs", word, Hasher.hash(url), normalizeTf(count, maxCount.get()));
                }
            }
            var tfsIter = kvs.scan("tfs");
            while(tfsIter.hasNext())
            {
                Row currRow = tfsIter.next();
                wordUrlCnt.put(currRow.key(), currRow.columns().size());
            }
            // sort the recentUse from low recent to high
            List<String> sortedMap = wordUrlCnt.entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByValue())
                    .map(Map.Entry::getKey)
                    .toList();
            // if exceed the key limit
            if(sortedMap.size() > IN_MEM_WORD)
            {
                List<String> rowsToStore = sortedMap.subList(0,sortedMap.size()-IN_MEM_WORD);
                // move the in-mem row to pt-row
                for(String rowKey:rowsToStore)
                {
                    if(kvs.existsRow("tfs",rowKey))
                    {
                        Row currRow = kvs.getRow("tfs",rowKey);
                        kvs.putRow("pt-tfs",currRow);
                        kvs.deleteInMemRow("tfs",rowKey);
                    }
                    else
                    {
                        System.out.println("doesn't have:"+rowKey);
                    }
                }
            }
            return new ArrayList<>();
        });

        // Save in-mem tfs to disk
        KVSClient kvs = new KVSClient(kvsAddress);
        var tfsIter = kvs.scan("tfs");
        while(tfsIter.hasNext())
        {
            Row currRow = tfsIter.next();
            if(currRow.columns().size()>0)
                kvs.putRow("pt-tfs",currRow);
        }
        kvs.delete("tfs");

        // Compute idf factor
        int N = kvs.count("pt-crawl");
        pairRdd = context.fromTable("pt-tfs", (row) -> {
            double n = row.columns().size();
            return row.key()+","+ Math.log(N / n);
        }).mapToPair((str) -> {
            String[] pair = str.split(",", 2);
            return new FlamePair(pair[0], pair[1]);
        });
        pairRdd.saveAsTable("pt-idfs");

        // Compute weights
        Row sumRow = new Row("firstSum");
        kvs.putRow("pt-weights", sumRow);
        pairRdd.flatMapToPair((pair) -> {
            String word = pair._1();
            String value = pair._2();
            double idf = Double.parseDouble(value);

            KVSClient kvss = new KVSClient(kvsAddress);
            Row weights = new Row(word);
            Row tfs = kvss.getRow("pt-tfs", word);

            Row firstSum = kvss.getRow("pt-weights", "firstSum");
            double secondSum = 0;
            for (String url : tfs.columns()) {
                value = new String(tfs.get(url));
                double tf = Double.parseDouble(value);
                double w = tf * idf;
                weights.put(url, String.valueOf(w));
                double prev = 0;
                if (firstSum.columns().contains(url)) {
                    prev = Double.parseDouble(firstSum.get(url));
                }
                firstSum.put(url, String.valueOf(prev + w * w));
                kvss.putRow("pt-weights", firstSum);
                secondSum += w * w;
            }
            weights.put("secondSum", String.valueOf(secondSum));
            kvss.putRow("pt-weights", weights);
            return new ArrayList<>();
        });
        
        context.output("OK");
    }

    private static Set<String> loadDictionary() throws IOException {
        Path p = Paths.get("data/web2");
        List<String> words = Files.readAllLines(p).stream().map(s->s.toLowerCase()).collect(Collectors.toList());
        return new HashSet<>(words);
    }

    private static Set<String> loadStopwords() throws IOException {
        Path p = Paths.get("data/stopwords");
        List<String> words = Files.readAllLines(p);
        return new HashSet<>(words);
    }

    private static Set<String> pageToWords(String page) {
        // Remove html tags
        page = page.replaceAll("<[^<]+>", " ");
        // Remove all punctuations
        page = page.replaceAll("\\p{Punct}", " ");
        // Remove multiple space
        page = page.replaceAll("[\\p{Space}]+", " ");
        // Heuristics
        page = page.replace("'", "");
        page = page.replace("-", " ");
        // To lower case
        page = page.toLowerCase();
        // Split to words
        String[] wordArr = page.split(" ");
        Set<String> words = new HashSet<>(Arrays.asList(wordArr));
        return words;
    }

    private static boolean filter(Set<String> stopwords, Set<String> dictionary, String word) {
        // Filter empty words
        if (word.equals("")) {
            return true;
        }
        if (!word.matches("[a-z]+")) {
            return true;
        }
        if (dictionary.contains(word)) {
            if (stopwords.contains(word)) {
                return true;
            }
            return false;
        }
        if (word.length() > 30) {
            return true;
        }
        return false;
    }

    private static String normalizeTf(double count, double maxCount) {
        double a = 0.5;
        double tf = a + (1 - a) * (count / maxCount);
        return String.valueOf(tf);
    }

    public static void main(String[] args) throws Exception {
        // Test 1
        // String url = "http://advanced.crawltest.cis5550.net/";
        // // String url = "https://en.wikipedia.org/wiki/Computer_science";
        // URL link = new URI(url).toURL();
        // HttpURLConnection conn = (HttpURLConnection) link.openConnection();
        // conn.setRequestMethod("GET");
        // conn.connect();
        // InputStream inputStream = conn.getInputStream();
        // byte[] page = inputStream.readAllBytes();
        // Set<String> words = pageToWords(new String(page));
        // for (String word : words) {
        //     System.out.print(word + ", ");
        // }
        // Test 2
        // List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 4, 3, 2, 1);
        // Map<Integer, Long> map = list.stream().collect(Collectors.groupingBy(x->x, Collectors.counting()));
        // for (int i : map.keySet()) {
        //     System.out.println(i + " " + map.get(i));
        // }
        // System.out.println(String.valueOf(!"a_1".matches("[a-z]+")));
        // Test 3
        String s = "shouldn't";
        PorterStemmer stemmer = new PorterStemmer();
        stemmer.add(s.toCharArray(), s.length());
        stemmer.stem();
        System.out.println(stemmer.toString());
    }
}
