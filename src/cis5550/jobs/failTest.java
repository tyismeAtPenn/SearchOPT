package cis5550.jobs;


import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.tools.Hasher;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;

import java.io.IOException;
import java.net.SocketException;
import java.net.URL;
import java.net.HttpURLConnection;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
public class failTest {

    public static void main(String[] args) throws IOException {
        String seed = "https://archive.ph/20130102094911/http://www.armytimes.com/offduty/travel/airforce_history_120808w/";

        URL urlObj = null;
        int responseCode = 0, statusCode = 0;
        try {
            urlObj = new URL(seed);
        } catch (Exception e) {
            e.printStackTrace();
        }

        URLManager um = new URLManager(seed);

        if (statusCode == RuleManager.DENY) {
            // Rule manager denied access
            System.out.println("Rule manager denied access");
        } else if (statusCode == RuleManager.DELAY) {
            // Rule manager delayed access
            System.out.println("Rule manager delayed access");
        }

        Row row = new Row(Hasher.hash(seed));
        row.put("url", seed);

        // HEAD request
        HttpURLConnection headReq = (HttpURLConnection) urlObj.openConnection();
        headReq.setRequestMethod("HEAD");
        headReq.setRequestProperty("User-Agent", "cis5550-crawler");
        headReq.setInstanceFollowRedirects(false);

        try {
            responseCode = headReq.getResponseCode();
        }
        catch (SocketException e)
        {
            System.out.println("fail to connect:"+seed);

        }
        row.put("responseCode", String.valueOf(responseCode));

        // HEAD: Error occurred
        if (responseCode != HttpURLConnection.HTTP_OK && responseCode != HttpURLConnection.HTTP_MOVED_PERM
                && responseCode != HttpURLConnection.HTTP_MOVED_TEMP
                && responseCode != HttpURLConnection.HTTP_SEE_OTHER
                && responseCode != 307 && responseCode != 308) {
            System.out.println("HEAD: Error occurred");
        }



        // HEAD: Handle content type and length
        String contentType = headReq.getContentType();
        String contentLength = headReq.getHeaderField("Content-Length");
        headReq.disconnect();


        // GET request

        HttpURLConnection getReq = (HttpURLConnection) urlObj.openConnection();
        getReq.setRequestMethod("GET");
        getReq.setRequestProperty("User-Agent", "cis5550-crawler");

        responseCode = getReq.getResponseCode();
        row.put("responseCode", String.valueOf(responseCode));

        // GET: Error occurred
        if (responseCode != HttpURLConnection.HTTP_OK) {

            System.out.println("GET: Error occurred");
        }

        // GET: Extract urls and store page
        String page = new String(getReq.getInputStream().readAllBytes());
        System.out.println(page);

        getReq.disconnect();





    }
}
