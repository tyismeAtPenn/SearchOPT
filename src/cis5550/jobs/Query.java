package cis5550.jobs;

import java.util.*;
import java.io.*;
import java.nio.file.Paths;
import java.util.concurrent.*;

import cis5550.flame.*;
import cis5550.kvs.KVS;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.external.PorterStemmer;
import cis5550.tools.KeyEncoder;

import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.spell.LuceneDictionary;
import org.apache.lucene.search.spell.PlainTextDictionary;
import org.apache.lucene.search.spell.SpellChecker;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class Query {
    // the stop words is from https://github.com/apache/lucene/blob/main/lucene/analysis/common/src/java/org/apache/lucene/analysis/en/EnglishAnalyzer.java
    private static ArrayList<String> stopWords = new ArrayList<String>(Arrays.asList("i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "you're", "you've", "you'll",
            "you'd", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "she's", "her", "hers", "herself", "it", "it's",
            "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "that'll", "these", "those",
            "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the",
            "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through",
            "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further",
            "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such",
            "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "don't", "should", "should've",
            "now", "d", "ll", "m", "o", "re", "ve", "y", "ain", "aren", "aren't", "couldn", "couldn't", "didn", "didn't", "doesn", "doesn't", "hadn",
            "hadn't", "hasn", "hasn't", "haven", "haven't", "isn", "isn't", "ma", "mightn", "mightn't", "mustn", "mustn't", "needn", "needn't", "shan",
            "shan't", "shouldn", "shouldn't", "wasn", "wasn't", "weren", "weren't", "won", "won't", "wouldn", "wouldn't"));
    private static String m_localhost = "localhost:8080";
    private static double wij_based = 1.0;
    private static double wiq_based = 0.0;

    // pre-fetch all Wij that i in Q for quick computation
    private static HashMap<String, Row> prefetchWIJ = new HashMap<>();
    private static  Row prefetchBased = null;

    // main use for Search
    public static String QuerySearch(String queryLine, String localhost) {
        // parse the query line
        m_localhost = localhost;
        ArrayList<String> queryWords = parseQueryWord(queryLine);
        // compute the tf-idf score for each word in the query line Wiq
        Map<String, Double> WIQ = ComputeWIQ(queryWords);
        // compute the ranking score
        PriorityQueue<ScoreEntry> scoreHeap = ComputeALLDocumentScore(queryWords, WIQ);
        // return the top X
        ArrayList<String> links = new ArrayList<>();
        while (!scoreHeap.isEmpty()) {
            System.out.println(scoreHeap.peek().url + " " + scoreHeap.peek().score);
            links.add(scoreHeap.poll().url);
        }
        // EC spell check
        String checkString = MyChecker.correctSentense(queryLine);

        if (checkString!= null){
            links.add("NEEDSPELLCHECK");
        }
        Collections.reverse(links);
        return StringToJsonArray(links);
    }

    // parse QueryWord to stemmed word list
    public static ArrayList<String> parseQueryWord(String queryWord) {
        queryWord = queryWord.replaceAll("[.,:;!?’\"()-]", "");
        queryWord = queryWord.toLowerCase();
        String[] rawWords = queryWord.split("\\s+");
        PorterStemmer stemmer = new PorterStemmer();
        ArrayList<String> queryWords = new ArrayList<>();
        for (String word: rawWords) {
            if (stopWords.contains(word) || word.isEmpty() ) continue;

            word = word.replaceAll(" ", "");
            stemmer.add(word.toCharArray(),word.length());
            stemmer.stem();
            String stemmedWord = stemmer.toString();

            if (stopWords.contains(stemmedWord)||stemmedWord.isEmpty()) continue;

            queryWords.add(stemmedWord);
        }
        return queryWords;
    }

    // FOR WIQ computation
    private  static Map<String, Double>  ComputeWIQ(ArrayList<String> queryWords) {
        // compute the tf score for each word in the query line Wiq
        Map<String, Double> TF = ComputeNormalizedTF(queryWords, 0.4);
        double sum = 0.0;
        Map<String, Double> WIQ = new HashMap<>();
        for (String word: queryWords) {
            //  get the word's idf
            double idf = getIDF(word);
            // compute the tf-idf score for each word in the query line Wiq
            sum += Math.pow(TF.get(word) * idf, 2);
            WIQ.put(word, TF.get(word) * idf);
        }
        wiq_based = sum;
        return WIQ;
    }

    private static Map<String, Double> ComputeNormalizedTF(ArrayList<String> queryWords, Double alpha) {
        // compute the tf score for each word in the query line Wiq
        Map<String, Double> tf = new HashMap<>();
        double maxFreq = 0.0;
        for (String word: queryWords) {
            tf.put(word, tf.getOrDefault(word, 0.0) + 1.0);
            if (tf.get(word) > maxFreq) {
                maxFreq = tf.get(word);
            }
        }
        // normalize the tf score
        for (String word: tf.keySet()) {
            tf.put(word, alpha + (1.0 - alpha) * tf.get(word) / maxFreq);
        }

        return tf;
    }



    // pre-fetch all Wij that i in Q for quick computation
    private static void prefetchAllWij(ArrayList<String> queryWords) {
        // fetch all need Wij table (i \in QueryWords)
        // TODO add right pt name
        KVSClient kvs = new KVSClient(m_localhost);
        for (String word: queryWords) {
            try {
                prefetchWIJ.put(word, kvs.getRow("pt-weights", word));
            } catch (Exception e) {
                System.out.println("ERROR: cannot find the weight table for the word " + word);
            }
        }
    }

    // pre-fetch all Wij based for all document in row firstSum
    private static void prefetchAllWijBased() {
        // fetch all need Wij table (i \in QueryWords)
        // TODO add right pt name
        KVSClient kvs = new KVSClient(m_localhost);
        try {
            prefetchBased = kvs.getRow("pt-weights", "firstSum");
        } catch (Exception e) {
            System.out.println("ERROR: cannot find the firstSum row");
        }
    }

    // get table methods
    private  static Double getIDF(String word) {
        // get the idf from the kvs
        KVSClient kvs = new KVSClient(m_localhost);
        try {
            Row row = kvs.getRow("pt-idfs", word);
            return Double.parseDouble(row.get(word));
        } catch (Exception e) {
//            System.out.println("ERROR: cannot find the idf for the word " + word);
            return 0.15;
        }
    }

    private static Double getTF(String word, String hashUrl) {
        // get the tf from the kvs
        KVSClient kvs = new KVSClient(m_localhost);
        try {
            Row row = kvs.getRow("pt-tfs", word);
            return Double.parseDouble(row.get(hashUrl));
        } catch (Exception e) {
//            System.out.println("ERROR: cannot find the tf for the word " + word + " and hashurl " + hashUrl);
            return 0.0;
        }
    }

    private static Double getWIJ(String word, String hashUrl) {
        // get the wij from the kvs
        KVSClient kvs = new KVSClient(m_localhost);
        try {
            // prev
//            Double tf = getTF(word, hashUrl);
//            Double idf = getIDF(word);
//            Double wij = tf * idf;
            // speed up 1 get from prefetch one
            double wij = Double.parseDouble(prefetchWIJ.get(word).get(hashUrl));
            return wij;
        } catch (Exception e) {
//            System.out.println("ERROR: cannot find the wij for the word " + word + " and hashurl " + hashUrl);
            return 0.000000001;
        }

    }

    private static Double getWIJBased(String hashUrl) {
        // get the wij from the kvs
        KVSClient kvs = new KVSClient(m_localhost);
        try {
            return Double.parseDouble(prefetchBased.get(hashUrl));
        } catch (Exception e) {
//            System.out.println("ERROR: cannot find the based for hashurl " + hashUrl);
            return 200.0;
        }

    }

    private  static Double getPageRankScore(String hashUrl) {
        // get the page rank score from the kvs
        KVSClient kvs = new KVSClient(m_localhost);
        try {
            Row row = kvs.getRow("pt-pageranks", hashUrl);
            return Double.parseDouble(row.get("rank"));
        } catch (Exception e) {
//            System.out.println("ERROR: cannot find the page rank score for the hashurl " + hashUrl);
            return 0.0;
        }
    }

    private static Double combineScore(String hashUrl, Double cosineScore) {
        // combine the score
        Double pageRankScore = 0.0;
        try {
            pageRankScore = getPageRankScore(hashUrl);
        } catch (Exception e) {
//            System.out.println("ERROR: cannot find the page rank score for the hashurl " + hashUrl);
            return cosineScore;
        }
        int CASE = 1;
        switch (CASE) {
            case 1: // only cosine score
                return cosineScore;
            case 2: // multiply
//                System.out.println("cosineScore: " + cosineScore + " pageRankScore: " + pageRankScore);
                return  cosineScore * pageRankScore;
            case 3: // weighted cosine score
                return  0.5 * cosineScore + 0.2 * pageRankScore;
            case 4: // harmonic mean
                return  2 * cosineScore * pageRankScore / (cosineScore + pageRankScore);
            default:
                return cosineScore;
        }

    }

    private static Double ComputeRankScore(String url, ArrayList<String> queryWords, Map<String, Double> WIQ) {
        // compute the ranking score
        double score = 0.0;

        for (String word: queryWords) {
            double wij = getWIJ(word, url);
            double wiq = WIQ.get(word);
            score += wij * wiq;
        }
        double base = Math.sqrt(getWIJBased(url)) * Math.sqrt(wiq_based);
//        System.out.println("score: " + score + " base: " + base);
        double combinedScore = combineScore(url, score / base);
        return combinedScore;
    }

    static class ScoreEntry {
        String url;
        Double score;

        ScoreEntry(String url, double score) {
            this.url = url;
            this.score = score;
        }
    }

    private static PriorityQueue<ScoreEntry> ComputeALLDocumentScore(ArrayList<String> queryWords, Map<String, Double> WIQ) {
        // compute the ranking score for all the documents
        KVSClient kvs = new KVSClient(m_localhost);
        // fetch all need Wij table (i \in QueryWords) and WIJ based
        prefetchAllWij(queryWords);
        prefetchAllWijBased();
        // TODO add right pt name
        Iterator<Row> iter = null;
        try {
            iter = kvs.scan("pt-pageranks");
        } catch (Exception e) {
            System.out.println("ERROR: cannot find the pageRank table");
            return null;
        }
        PriorityQueue<ScoreEntry> scoreHeap = new PriorityQueue<>((a, b) -> Double.compare(a.score, b.score));

        class ScoreTask implements Callable<ScoreEntry> {
            private Row row;

            public ScoreTask(Row row) {
                this.row = row;
            }

            @Override
            public ScoreEntry call() throws Exception {
                Double score = ComputeRankScore(row.key(),queryWords,WIQ);
                return new ScoreEntry(row.key(),score);
            }
        }
        ExecutorService executorService = Executors.newFixedThreadPool(5);

        List<Future<ScoreEntry>> futures = new ArrayList<>();
        while(iter.hasNext())
        {
            Callable<ScoreEntry> task = new ScoreTask(iter.next());
            Future<ScoreEntry> future = executorService.submit(task);
            futures.add(future);
        }

        // wait for join
        executorService.shutdown();

        for (Future<ScoreEntry> future : futures) {
            try {
                if (scoreHeap.size() < 40) {
                    scoreHeap.add(future.get());
                }
                else if (scoreHeap.peek().score < future.get().score) {
                    scoreHeap.poll();
                    scoreHeap.add(future.get());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println("scoreHeap size:"+scoreHeap.size());

//        while (iter.hasNext()) {
//            Row row = iter.next();
//            String url = row.key();
//            double score = ComputeRankScore(url, queryWords, WIQ);
//            // TODO adjust the size of the heap
//            if (scoreHeap.size() < 40) {
//                scoreHeap.add(new ScoreEntry(url, score));
//            }else{
//                if (scoreHeap.peek().score < score) {
//                    scoreHeap.poll();
//                    scoreHeap.add(new ScoreEntry(url, score));
//                }
//            }
//        }

        return scoreHeap;
    }

    // EC spellcheck return a "Do you mean XXX"
    public class MyChecker {

        static SpellChecker spellChecker = null;

        public static String correctSentense(String queryLine){
            initChecker();

            String[] words = queryLine.split("\\s+");
            StringBuilder sb = new StringBuilder();
            int correctedCNT = 0;
            for (String word : words) {
                try {
                    String afterCorrected = spellCheck(word);
                    sb.append(afterCorrected).append(" ");
                    if (!afterCorrected.equals(word))
                        correctedCNT += 1;

                } catch (Exception e) {
                    sb.append(word).append(" ");
                }
            }
            System.out.println(sb.toString());
            closeChecker();
            if (correctedCNT != 0 ){
                return sb.toString();
            }
            return null;

        }

        public static void initChecker(){
            try {
                Directory directory = FSDirectory.open(Paths.get("spellchecker/"));
                spellChecker = new SpellChecker(directory);

                spellChecker.indexDictionary(new PlainTextDictionary(Paths.get("../data/web2")),
                    new IndexWriterConfig(), false);
            } catch (Exception e){
                System.out.println("ERROR: cannot init spellchecker");
            }
        }

        public static void closeChecker(){
            try {
                spellChecker.close();
            } catch (Exception e){
                System.out.println("ERROR: cannot close spellchecker");
            }
        }

        public static String spellCheck(String word) throws Exception {
            String[] suggestions = spellChecker.suggestSimilar(word, 1);
            return suggestions[0];
        }

    }

    private static String StringToJsonArray(ArrayList<String> links) {
        StringBuilder jsonArrayBuilder = new StringBuilder("[");
        for (int i = 0; i < links.size(); i++) {
            jsonArrayBuilder.append("{\"url\":\"").append(links.get(i)).append("\"}");
            if (i < links.size() - 1) {
                jsonArrayBuilder.append(",");
            }
        }
        jsonArrayBuilder.append("]");

        String jsonArrayString = jsonArrayBuilder.toString();

//        System.out.println(jsonArrayString);
        return jsonArrayString;
    }

    public static void main(String[] args) throws IOException {
        // Directory directory = FSDirectory.open(Paths.get("spellchecker/"));
        // SpellChecker spellChecker = new SpellChecker(directory);
        // spellChecker.indexDictionary(
        //         new PlainTextDictionary(Paths.get("data/web2")), new IndexWriterConfig(), false);
        // String wordForSuggestions = "hwllo";
        // int suggestionsNumber = 5;

        // String[] suggestions = spellChecker.
        //     suggestSimilar(wordForSuggestions, suggestionsNumber);
        // spellChecker.close();

        // if (suggestions != null && suggestions.length > 0) {
        //     for (String word : suggestions) {
        //         System.out.println("Did you mean:" + word);
        //     }
        // }

        String query = "applc";
        String corrected = Query.MyChecker.correctSentense(query);
        System.out.println(corrected);
    }
}
