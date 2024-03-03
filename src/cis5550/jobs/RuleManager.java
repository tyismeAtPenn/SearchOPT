package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameContextImpl;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.URLParser;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * This class manages the crawler rules for a given host, including the crawl delay, host rules, and
 * blacklist.
 * @author Lang Qin
 */
public class RuleManager {

    // Constants
    public static final String RULE_TABLE = "RulesByHost";
    public static final int ALLOW = 0;
    public static final int DENY = 1;
    public static final int DELAY = 2;
    private static final int MIN_CRAWL_DELAY = 1000;
    private static final String LAST_ACCESS_TIME = "lastAccessTime";
    private static final String CRAWL_DELAY = "crawlDelay";
    private static final String RULES = "rules";
    private static final String PATTERN = "pattern";
    private static final String AGENT_NAME = "cis5550-crawler";
    private static final List<String> FORBIDDEN_KEY_WORDS= Arrays.asList("npr", "cnn", "twitter", "tel", "linkedin", "facebook");

    private final KVSClient kvs;

    private final String[] hostParts;

    private final List<Rule<Boolean, String>> rules;

    private final List<String> blacklist;

    private int crawlDelay;

    private long lastAccessTime;

    /**
     * Rule Record.
     *
     * @param <Boolean> type of rule
     * @param <String> type of path
     */
    public record Rule<Boolean, String>(Boolean isAllow, String path) {}

    /**
     * Constructor
     * </>
     * This class accepts an optional info table name. If the info table name is provided, the
     * constructor will fetch the blacklist from the info table.
     *
     * @param kvsAddr The coordinator address of kvs
     * @param host The host URL (normalized)
     * @param infoTable The table name in which the blacklist is stored
     */
    public RuleManager(String kvsAddr, String host, String infoTable) {
        this.kvs = new KVSClient(kvsAddr);
        this.hostParts = URLParser.parseURL(host);
        this.rules = new ArrayList<>();
        this.crawlDelay = -1;

        if (infoTable != null) {
            this.blacklist = fetchBlackList(infoTable);
        } else {
            this.blacklist = new ArrayList<>();
        }
        initialize();
        sync();
    }

    /**
     * Fetch the crawler rules from either the host or our storage. If no rules are found,
     * the default crawl delay will be set to 1 second.
     */
    public void initialize() {
//      if  kvs.existsRow(RULE_TABLE, hostParts[1])
        if (isVisitedHost()) {
            initializeFromTable();
        } else {
            initializeFromHost();
        }

        if (this.crawlDelay == -1) {
            this.crawlDelay = 1000;
        }
    }

    /**
     * Synchronize the rule status to our storage.
     */
    public void sync() {
        Row r = new Row(hostParts[1]);
        r.put(LAST_ACCESS_TIME, String.valueOf(lastAccessTime));
        r.put(CRAWL_DELAY, String.valueOf(crawlDelay));

        StringBuilder sb = new StringBuilder();
        for (Rule<Boolean, String> rule : rules) {
            sb.append(rule.isAllow() ? "Allow: " : "Disallow: ").append(rule.path()).append("\n");
        }
        r.put(RULES, sb.toString());

        try {
            this.kvs.putRow(RULE_TABLE, r);
        } catch (IOException e) {
            throw new RuntimeException("Failed to sync with KVS.");
        }
    }

    /**
     * Check if the given URL is allowed to be crawled:
     *     1. blacklist, 2. allowed/disallowed, 3. crawl delay
     * This method assumes user will access the URL immediately after calling this method.
     * Update the last access time if the URL is allowed to be crawled.
     *
     * @param url The URL to be checked
     * @return 1 if the URL is not allowed to be crawled, 2 if the URL is allowed to be crawled but
     *        the crawl delay is not met, 0 otherwise
     */
    public int isLegalThenAccess(String url) {
        if (isBlacklisted(url)) {
            return DENY;
        }

        if (!isAllowed(url)) {
            return DENY;
        }

        if (System.currentTimeMillis() - getLastAccessTime() < crawlDelay) {
            return DELAY;
        }

        this.lastAccessTime = System.currentTimeMillis();
        updateLastAccessTime();
        return ALLOW;
    }

    /**
     * Update the last access time of the host in our storage.
     * @param lastAccessTime The last access time of the host
     */
    public void updateLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
        updateLastAccessTime();
    }

    /**
     * [Helper Method]
     * Update the last access time of the host in our storage. This method assumes the host has been
     * visited before and the last access time is tracked in the field parameter lastAccessTime.
     */
    private void updateLastAccessTime() {
        try {
            kvs.put(RULE_TABLE, hostParts[1], LAST_ACCESS_TIME, String.valueOf(lastAccessTime));
        } catch (IOException e) {
            throw new RuntimeException("Failed to update last access time in KVS.");
        }
    }

    /**
     * [Helper Method]
     * Get the last access time of the host from our storage. This method assumes the host has been
     * visited before and the last access time is stored in the table.
     *
     * @return The last access time of the host
     */
    private long getLastAccessTime() {
        try {
            Row r = kvs.getRow(RULE_TABLE, hostParts[1]);
            return Long.parseLong(r.get(LAST_ACCESS_TIME));
        } catch (IOException e) {
            throw new RuntimeException("Failed to get last access time from KVS.");
        }
    }

    /**
     * [Helper Method]
     * Check if the given URL is blacklisted.
     *
     * @param url The URL to be checked
     * @return true if the URL is blacklisted, false otherwise
     */
    private boolean isBlacklisted(String url) {
        for (String pattern : this.blacklist) {
            String regex = pattern.replace("*", ".*");
            if (url.matches(regex)) {
                return true;
            }
        }
        return false;
    }

    /**
     * [Helper Method]
     * Check if the given URL is allowed to be crawled.
     *
     * @param url The URL to be checked
     * @return true if the URL is allowed to be crawled, false otherwise
     */
    private boolean isAllowed(String url) {
        String[] parts = URLParser.parseURL(url);
        if (parts.length < 4) {
            return false;
        }
        String path = parts[3];

        for (Rule<Boolean, String> rule : this.rules) {
            if (path.startsWith(rule.path())) {
                return rule.isAllow();
            }
        }
        return true;
    }

    /**
     * [Helper Method]
     * Fetch the crawl rules from robots.txt file on the host.
     */
    private void initializeFromHost() {
        String robots;
        if ((robots = fetchRobotsFromHost()) == null) {
            return;
        }

        if (robots.isEmpty()) {
            return;
        }

        String[] rulesByGroups = robots.split("\\n\\n");

        String paragraph;
        if ((paragraph = getSpecificGroup(rulesByGroups, AGENT_NAME)) == null) {
            if ((paragraph = getSpecificGroup(rulesByGroups, "*")) == null) {
                return;
            }
        }

        parseRules(paragraph);
    }

    /**
     * [Helper Method]
     * Parse and store the rules from the group of rules in string format.
     *
     * @param rule The group of rules in string format
     */
    private void parseRules(String rule) {
        String[] lines = rule.split("\\n");
        for (String line : lines) {
            if (line.startsWith("#")) {
                continue;
            }

            if (line.startsWith("Allow:") || line.startsWith("Disallow:")) {
                Rule<Boolean, String> r;
                if ((r = stringToRecord(line)) != null) {
                    if(!r.path.isEmpty())
                        this.rules.add(r);
                }
            }

            if (line.startsWith("Crawl-delay:")) {
                String delayString = line.split(":")[1].trim();
                int delay = (int) (Float.parseFloat(delayString) * 1000);
                this.crawlDelay = Math.max(delay, MIN_CRAWL_DELAY);
            }
        }
    }

    /**
     * [Helper Method]
     * Select the specific group of rules from the robots.txt file in string format.
     *
     * @param rulesByGroups The robots.txt file in string format by groups separated by user-agent
     * @param userAgent The user-agent to be selected
     * @return The specific group of rules in string format; null if the group does not exist
     */
    static String getSpecificGroup(String[] rulesByGroups, String userAgent) {
        for (String group : rulesByGroups) {
            if (group.contains("User-agent: " + userAgent)) {
                return group;
            }
        }
        return null;
    }

    /**
     * Request the robots.txt file from the host.
     *
     * @return The robots.txt file in string format; null if the request fails.
     */
    private String fetchRobotsFromHost() {
        String robots;

        try {
            String robotsURL = hostParts[0] + "://" + hostParts[1] + ":" + hostParts[2] + "/robots.txt";
            HttpURLConnection robotsReq = (HttpURLConnection) new URL(robotsURL).openConnection();
            robotsReq.setRequestMethod("GET");
            robotsReq.setRequestProperty("User-Agent", AGENT_NAME);
            robotsReq.setInstanceFollowRedirects(false);

            this.lastAccessTime = System.currentTimeMillis();
            if (robotsReq.getResponseCode() != HttpURLConnection.HTTP_OK) {
                return null;
            }

            robots = new String(robotsReq.getInputStream().readAllBytes());
            this.lastAccessTime = System.currentTimeMillis();
        } catch (Exception e) {
            return null;
        }

        return robots;
    }

    /**
     * Fetch the rules from our storage. This method assumes the host has been visited before and
     * the rules are stored in the table.
     *
     */
    private void initializeFromTable() {
        try {
            Row r = kvs.getRow(RULE_TABLE, hostParts[1]);
            if(!(r.get(RULES) == null) && !r.get(RULES).isEmpty())
            {
                for (String rule : r.get(RULES).split("\\n")) {
                    this.rules.add(stringToRecord(rule));
                }
            }
            this.crawlDelay = Integer.parseInt(r.get(CRAWL_DELAY));
            this.lastAccessTime = Long.parseLong(r.get(LAST_ACCESS_TIME));
        } catch (IOException e) {
            throw new RuntimeException("Failed to fetch rules from KVS.");
        }
    }

    /**
     * [Helper Method]
     * Convert a rule in string format to a rule record.
     *
     * @param line The rule in string format
     * @return The rule record
     */
    private static Rule<Boolean, String> stringToRecord(String line) {
        String[] parts = line.split(":");
        if (parts.length != 2) {
            return null;
        }
        String type = parts[0].trim();
        String path = parts[1].trim();

        if (type.equals("Allow")) {
            return new Rule<>(true, path);
        } else {
            return new Rule<>(false, path);
        }
    }

    /**
     * [Helper Method]
     * Check if the host has been visited before.
     *
     * @return true if the host has been visited before, false otherwise
     */
    private boolean isVisitedHost() {
        try {
            return kvs.existsRow(RULE_TABLE, hostParts[1]);
        } catch (IOException e) {
            throw new RuntimeException("Failed to check with KVS.");
        }
    }

    /**
     * [Helper Method]
     * Fetch the blacklist from the info table.
     *
     * @param table The table name in which the blacklist is stored
     * @return a list of blacklisted patterns
     */
    private List<String> fetchBlackList(String table) {
        List<String> result = new ArrayList<>();
        try {
            Iterator<Row> itr = kvs.scan(table);
            while (itr.hasNext()) {
                Row row = itr.next();
                result.add(row.get(PATTERN));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to fetch blacklist from KVS.");
        }
        return result;
    }

    public static void main(String[] args) {
        String robot = "Sitemap: https://wikimediafoundation.org/sitemap.xml\n" +
                "Sitemap: https://wikimediafoundation.org/news-sitemap.xml\n" +
                "# START YOAST BLOCK\n" +
                "# ---------------------------\n" +
                "User-agent: *\n" +
                "Disallow:\n" +
                "\n" +
                "# ---------------------------\n" +
                "# END YOAST BLOCK";
        String[] rulesByGroups = robot.split("\\n\\n");

        String paragraph;
        if ((paragraph = getSpecificGroup(rulesByGroups, AGENT_NAME)) == null) {
            if ((paragraph = getSpecificGroup(rulesByGroups, "*")) == null) {
                return;
            }
        }

        List<Rule<Boolean, String>> rules = new ArrayList<>();
        String[] lines = paragraph.split("\\n");
        for (String line : lines) {
            if (line.startsWith("#")) {
                continue;
            }

            if (line.startsWith("Allow:") || line.startsWith("Disallow:")) {
                Rule<Boolean, String> r;
                System.out.println("start with allow/disallow");
                if ((r = stringToRecord(line)) != null) {
                    if(!r.path.isEmpty())
                        rules.add(r);
                }
            }

            if (line.startsWith("Crawl-delay:")) {
                String delayString = line.split(":")[1].trim();
                int delay = (int) (Float.parseFloat(delayString) * 1000);
            }
        }
        for(var rule:rules)
            System.out.println("rule first:"+rule.isAllow+"rule second:"+rule.path);
        System.out.println("parrgraph:"+paragraph);
    }
}
