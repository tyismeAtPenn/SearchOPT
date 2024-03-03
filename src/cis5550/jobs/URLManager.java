package cis5550.jobs;

import cis5550.tools.URLParser;

/**
 * This class is used to normalize URLs for the crawler, providing a consistent format for URLs
 * based on the seed URL.
 * </>
 * Notice that this class provides two methods to normalize URLs. The first method is used to
 * normalize the seed URL. The second method is used to normalize URLs crawled from the seed URL.
 *
 * @author Lang Qin
 */
public class URLManager {

    // Constants
    private static final String HTTP = "http";
    private static final String HTTPS = "https";
    private static final String DEFAULT_PORT = "80";
    private static final String DEFAULT_SECURE_PORT = "443";

    /**
     * The seed URL in the format of [protocol]://[host]:[port][path]
     */
    private final String[] seedParts;

    /**
     * Constructor
     * </>
     * This class requires a seed URL to be initialized. If the seed URL is not in the format of
     * [protocol]://[host]:[port][path], it will be normalized.
     * </>
     * The seed URL will be stored in the format of [protocol]://[host]:[port][path] for later use.
     * @param seed The seed URL
     */
    public URLManager(String seed) {
        seedParts = normalizeSeedURL(seed);
    }

    /**
     * Get the string representation of given URL parts
     *
     * @param parts The URL in the format of [protocol]://[host]:[port][path]
     * @return The string representation of the URL
     */
    public String partsToString(String[] parts) {
        return parts[0] + "://" + parts[1] + ":" + parts[2] + parts[3];
    }

    /**
     * Get the string representation of the seed URL
     *
     * @return The string representation of the seed URL
     */
    public String getNormalizedSeedURL() {
        return partsToString(seedParts);
    }

    /**
     * Normalize the seed URL. This method assumes the seed URL uses either HTTP or HTTPS protocol.
     *
     * @param seed The seed URL
     * @return The normalized seed URL in the format of [protocol]://[host]:[port][path]
     */
    public String[] normalizeSeedURL(String seed) {
        if (seed.contains("#")) {
            seed = seed.substring(0, seed.indexOf("#"));
        }

        return normalizeWithHost(seed);
    }

    /**
     * Normalize and filter the crawled URL. URL manager will filter out URLs with protocols other
     * than HTTP and HTTPS, URLs with only fragments, and URLs with unwanted file extension.
     *
     * @param url The crawled URL string
     * @return the normalized URL string or null if the URL is filtered
     */
    public String processCrawledURL(String url) {
        int idx = url.indexOf("#");
        if (idx != -1 && (url = url.substring(0, idx)).isEmpty()) {
            // URL is only a fragment; ignore it
            return null;
        }

        String host = URLParser.parseURL(url)[0];
        if (host != null && !host.equals(HTTP) && !host.equals(HTTPS)) {
            // URL is not HTTP or HTTPS; ignore it
            return null;
        }

        if (url.endsWith(".jpg") || url.endsWith(".jpeg") || url.endsWith(".gif")
                || url.endsWith(".png") || url.endsWith(".txt")) {
            // URL has an invalid file extension; ignore it
            return null;
        }

        return normalizeCrawledURL(url);
    }

    /**
     * Normalize the crawled URL with the following steps:
     * 1. Cut off the fragment; if the URL becomes empty, return null
     * 2. Case 1 - URL comes with host: return the normalized URL
     *    Case 2 - URL is relative: resolve the relative path and return the normalized URL
     *    Case 3 - URL is absolute: append the host and port of the seed URL and return the normalized URL
     *
     * @return The normalized seed URL in the format of [protocol]://[host]:[port][path]
     */
    public String normalizeCrawledURL(String url) {
        if (url.startsWith(HTTP) || url.startsWith(HTTPS)) {
            // case 1 - URL comes with host
            return partsToString(normalizeWithHost(url));
        }

        if (url.startsWith("/")) {
            // case 2 - URL is absolute
            return partsToString(normalizeWithHost(seedParts[0] + "://" + seedParts[1] + ":" + seedParts[2] + url));
        }

        // case 3 - URL is relative
        return partsToString(resolveRelativePath(url));
    }

    /**
     * [Helper Method]
     * Normalize the absolute URL. This method assumes the URL uses either HTTP or HTTPS protocol.
     *
     * @param url The URL in string format
     * @return The normalized URL in the format of [protocol]://[host]:[port][path]
     * @throws IllegalArgumentException if the URL is not HTTP or HTTPS
     */
    private String[] normalizeWithHost(String url) {
        String[] parts = URLParser.parseURL(url);
        if (!parts[0].equals(HTTP) && !parts[0].equals(HTTPS)) {
            throw new IllegalArgumentException("URL must be HTTP or HTTPS");
        }

        if (parts[2] == null) {
            parts[2] = parts[0].equals(HTTP) ? DEFAULT_PORT : DEFAULT_SECURE_PORT;
        }

        if (parts[3] == null) {
            parts[3] = "/";
        }
        return parts;
    }

    /**
     * [Helper Method]
     * Resolve the relative path of the URL to the seed URL.
     *
     * @param url The URL in the format of [protocol]://[host]:[port][path]
     * @return The normalized URL in the format of [protocol]://[host]:[port][path]
     */
    private String[] resolveRelativePath(String url) {
        String[] urlParts = URLParser.parseURL(url);
        urlParts[0] = seedParts[0];
        urlParts[1] = seedParts[1];
        urlParts[2] = seedParts[2];

        String[] urlPathSegments = url.split("/");
        String[] seedPathSegments = seedParts[3].substring(1).split("/");

        int count = seedPathSegments.length - 1;
        for (String segment : urlPathSegments) {
            if (segment.equals("..")) {
                count--;
            }
        }

        StringBuilder resolvedPath = new StringBuilder();
        for (int i = 0; i < count; i++) {
            resolvedPath.append("/").append(seedPathSegments[i]);
        }
        for (String segment : urlPathSegments) {
            if (segment.equals("..")) {
                continue;
            }
            resolvedPath.append("/").append(segment);
        }

        urlParts[3] = resolvedPath.toString();
        return urlParts;
    }
}