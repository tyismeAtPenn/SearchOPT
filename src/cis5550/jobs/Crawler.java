package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.tools.Hasher;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;

import java.net.SocketException;
import java.net.URL;
import java.net.HttpURLConnection;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Crawler {
    private static final Pattern URL_PATTERN = Pattern.compile(
            "<a\\s+(?:[^>]*?\\s+)?href=\"([^\"]*)\"", Pattern.CASE_INSENSITIVE);
    private static final List<String> FORBIDDEN_KEY_WORDS
            = Arrays.asList(" ", "npr", "cnn", "twitter", "tel", "linkedin", "facebook", "library",
                "neighbors", "vimeo", "contact-us", "verify", "image", "cbc", "archive.today", "comscore", "theperfumeshop",
                "pinterest", "instagram", "youtube", "google", "yahoo", "bing", "amazon", "ebay", "walmart", "target",
                "cdon", "thesundaily", "uk.wikipedia", "mt.wikipedia", "tvnz", "reuters");

    private static final String TABLE_CRAWL = "pt-crawl";

    public static void run(FlameContext ctx, String[] args) throws Exception {
        if (args.length < 1) {
            ctx.output("Usage: Crawler <seed-url> [options]");
            return;
        }

        String url = new URLManager(args[0]).getNormalizedSeedURL();

        ctx.output("Crawling " + url);
        FlameRDD urlQueue;
        if(ctx.getKVS().existsRow("pt-checkpoint","frontierUrl"))
        {
            List<String> frontierUrlList = new ArrayList<>();
            Row frointierRow = ctx.getKVS().getRow("pt-checkpoint","frontierUrl");
            for(String col:frointierRow.columns())
            {
                frontierUrlList.add(frointierRow.get(col));
            }
            urlQueue = ctx.parallelize(frontierUrlList);
        }
        else
            urlQueue = ctx.parallelize(Collections.singletonList(url));
        String kvsAddr = ctx.getKVS().getCoordinator();

        while (urlQueue.count() > 0) {
            urlQueue = urlQueue.flatMap(seed -> {
                Thread.sleep(1000);
                if (!seed.contains("en.wikipedia.org")) {
                    return new ArrayList<>();
                }
//                for (String s : FORBIDDEN_KEY_WORDS) {
//                    if (seed.contains(s)) {
//                        System.out.println("url contains forbidden key words");
//                        return new ArrayList<>();
//                    }
//                }
                // Check if URL is already crawled
                System.out.println("seed:"+seed);
                KVSClient kvs = new KVSClient(kvsAddr);
                
                if (kvs.existsRow(TABLE_CRAWL, Hasher.hash(seed))) {
                    return new ArrayList<>();
                }

                int responseCode, statusCode;
                URL urlObj = new URL(seed);

                URLManager um;
                try {
                    um = new URLManager(seed);
                } catch (Exception e) {
                    return new ArrayList<>();
                }
                RuleManager rm = new RuleManager(kvsAddr, seed, args.length > 1 ? args[1] : null);

                try {
                    statusCode = rm.isLegalThenAccess(seed);
                } catch (Exception e) {
                    return new ArrayList<>();
                }

                if (statusCode == RuleManager.DENY) {
                    // Rule manager denied access
                    return new ArrayList<>();
                } else if (statusCode == RuleManager.DELAY) {
                    // Rule manager delayed access
                    return Collections.singletonList(seed);
                }

                Row row = new Row(Hasher.hash(seed));
                row.put("url", seed);

                // HEAD request
                HttpURLConnection headReq = (HttpURLConnection) urlObj.openConnection();
                headReq.setRequestMethod("HEAD");
                headReq.setRequestProperty("User-Agent", "cis5550-crawler");
                headReq.setInstanceFollowRedirects(false);
                headReq.setConnectTimeout(1000);
                headReq.setReadTimeout(5000);

                try {
                    responseCode = headReq.getResponseCode();
                }
                catch (Exception e)
                {
                    System.out.println("Timeout/fail to connect:"+seed);
                    return new ArrayList<>();
                }
                row.put("responseCode", String.valueOf(responseCode));

                // HEAD: Error occurred
                if (responseCode != HttpURLConnection.HTTP_OK && responseCode != HttpURLConnection.HTTP_MOVED_PERM
                        && responseCode != HttpURLConnection.HTTP_MOVED_TEMP
                        && responseCode != HttpURLConnection.HTTP_SEE_OTHER
                        && responseCode != 307 && responseCode != 308) {
                    kvs.putRow(TABLE_CRAWL, row);
                    return new ArrayList<>();
                }

                // HEAD: Handle redirects
                if (responseCode != HttpURLConnection.HTTP_OK) {
                    String newUrl = headReq.getHeaderField("Location");
                    kvs.putRow(TABLE_CRAWL, row);

                    String link;
                    try {
                        link = um.processCrawledURL(newUrl);
                    } catch (Exception e) {
                        return new ArrayList<>();
                    }

                    if (link == null) {
                        return new ArrayList<>();
                    }
                    return Collections.singletonList(link);
                }

                // HEAD: Handle content type and length
                String contentType = headReq.getContentType();
                String contentLength = headReq.getHeaderField("Content-Length");
                if (contentType != null) {
                    row.put("contentType", contentType);
                }
                if (contentLength != null) {
                    row.put("length", contentLength);
                }

                headReq.disconnect();

                // Request page if content type is text/html
                if (contentType == null || !contentType.startsWith("text/html")) {
                    kvs.putRow(TABLE_CRAWL, row);
                    return new ArrayList<>();
                }

                // GET request
                rm.updateLastAccessTime(System.currentTimeMillis());
                HttpURLConnection getReq = (HttpURLConnection) urlObj.openConnection();
                getReq.setRequestMethod("GET");
                getReq.setRequestProperty("User-Agent", "cis5550-crawler");
                getReq.setConnectTimeout(1000);
                getReq.setReadTimeout(10000);
                try {
                    responseCode = getReq.getResponseCode();
                }
                catch (Exception e)
                {
                    System.out.println("Timeout/fail to connect:"+seed);
                    return new ArrayList<>();
                }

                row.put("responseCode", String.valueOf(responseCode));

                // GET: Error occurred
                try{
                    responseCode = getReq.getResponseCode();
                }
                catch (Exception e)
                {
                    System.out.println("fail to connect:"+seed);
                    System.out.println(e.getMessage());
                    return new ArrayList<>();
                }

                if (responseCode != HttpURLConnection.HTTP_OK) {
                    kvs.putRow(TABLE_CRAWL, row);
                    return new ArrayList<>();
                }

                row.put("responseCode", String.valueOf(responseCode));
                // GET: Extract urls and store page
                String page = new String(getReq.getInputStream().readAllBytes());
                row.put("page", page);
                getReq.disconnect();
                kvs.putRow(TABLE_CRAWL, row);

                return new ArrayList<>();
            });
            // checkpoint here:
            List<String> currCheckpoint = urlQueue.collect();
            Row row = new Row("frontierUrl");
            for(int i=0;i< currCheckpoint.size();i++)
                row.put(String.valueOf(i),currCheckpoint.get(i));
            ctx.getKVS().putRow("pt-checkpoint",row);
            System.gc();
            Thread.sleep(1000);
        }
    }

    /**
     * [Helper Method]
     * Extracts URLs from a page.
     *
     * @param page HTML page
     * @return list of URLs
     */
    public static List<String> extractURLs(String page) {
        List<String> urls = new ArrayList<>();
        Matcher matcher = URL_PATTERN.matcher(page);

        while (matcher.find()) {
            urls.add(matcher.group(1));
        }

        return urls;
    }
}
