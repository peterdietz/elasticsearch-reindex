/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree and available online at
 *
 * http://www.dspace.org/license/
 */
package com.pannous.es.reindex;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.io.*;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SpiderDetector is used to find IP's that are spiders...
 * In future someone may add UserAgents and Host Domains
 * to the detection criteria here.
 *
 * @author kevinvandevelde at atmire.com
 * @author ben at atmire.com
 * @author Mark Diggory (mdiggory at atmire.com)
 * @author Kevin Van Ransbeeck at atmire.com
 */
public class SpiderDetector {

    private static Logger log = Logger.getLogger("test");

    /**
     * Sparse HAshTable structure to hold IP Address Ranges.
     */
    private static IPTable table = null;

    // UserAgent Regex set for matching.
    private static Set<Pattern> userAgentSpidersRegex = null;
    // Matched UserAgent set for faster matching.
    //private static Set<String> userAgentSpidersMatched = null;

    private static LoadingCache<String, String> userAgentSpidersMatched = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .build(new CacheLoader<String, String>() {
        @Override public String load(String key) throws Exception {
            return new String();
        }
    });


    // DomainName Regex set for matching.
    private static Set<Pattern> domainNameSpidersRegex = null;
    // Matched DomainName set for faster matching.
    //private static Set<String> domainNameSpiderMatched = null;
    private static LoadingCache<String, String> domainNameSpiderMatched = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .build(new CacheLoader<String, String>() {
        @Override public String load(String key) throws Exception {
            return new String();
        }
    });

    /**
     * Utility method which Reads the ip addresses out a file & returns them in a Set
     *
     * @param spiderIpFile the location of our spider file
     * @return a vector full of ip's
     * @throws IOException could not happen since we check the file be4 we use it
     */
    public static Set<String> readIpAddresses(File spiderIpFile) throws IOException {
        Set<String> ips = new HashSet<String>();

        if (!spiderIpFile.exists() || !spiderIpFile.isFile()) {
            return ips;
        }

        //Read our file & get all them ip's
        BufferedReader in = new BufferedReader(new FileReader(spiderIpFile));
        String line;
        while ((line = in.readLine()) != null) {
            if (!line.startsWith("#")) {
                line = line.trim();

                if (!line.equals("") && !Character.isDigit(line.charAt(0))) {
                    // is a hostname
                    // add this functionality later...
                } else if (!line.equals("")) {
                    ips.add(line);
                    // is full v4 ip (too tired to deal with v6)...
                }
            } else {
                //   ua.add(line.replaceFirst("#","").replaceFirst("UA","").trim());
                // ... add this functionality later
            }
        }
        in.close();
        return ips;
    }

    /**
     * Get an immutable Set representing all the Spider Addresses here
     *
     * @return Set<String> setOfIpAddresses
     */
    public static Set<String> getSpiderIpAddresses() {
        loadSpiderIpAddresses();
        return table.toSet();
    }

    /*
        private loader to populate the table from files.
     */

    private static void loadSpiderIpAddresses() {
        if (table == null) {
            table = new IPTable();

            String filePath = "/dspace/";
            try {
                File spidersDir = new File(filePath, "config/spiders");

                if (spidersDir.exists() && spidersDir.isDirectory()) {
                    for (File file : spidersDir.listFiles()) {
                        for (String ip : readIpAddresses(file)) {
                            table.add(ip);
                        }
                        log.info("Loaded Spider IP file: " + file);
                    }
                } else {
                    log.info("No spider file loaded");
                }
            } catch (Exception e) {
                log.log(Level.WARNING, "Error Loading Spiders:" + e.getMessage(), e);
            }
        }
    }



    /**
     * Check individual IP is a spider.
     *
     * @param ip
     * @return if is spider IP
     */
    public static boolean isSpiderByIP(String ip) {
        if (table == null) {
            SpiderDetector.loadSpiderIpAddresses();
        }

        try {
            if (table.contains(ip)) {
                return true;
            }
        } catch (Exception e) {
            return false;
        }

        return false;
    }

    public static boolean isSpiderByIPOrDomainNameOrUserAgent(String ipAddress, String dns, String userAgent) {
        return isSpiderByIPOrDomainNameOrUserAgent(null, ipAddress, dns, userAgent);
    }

    /**
     * Using the information we have, check against the enabled methods of spider detection to see if this is a spider
     * or human.
     * @param xForwardForIPs (optional) A proxy server could have caught this request if enabled, comma separated
     * @param ipAddress End Users IP address
     * @param dns (optional) The hostname + domain for the end user, if null, we'll do a reverse-lookup based on IP
     * @param userAgent What the client is claiming to be, i.e. Firefox or GoogleBot
     * @return boolean of whether they are detected as a spider or not. true=spider, false=human
     */
    public static boolean isSpiderByIPOrDomainNameOrUserAgent(String xForwardForIPs, String ipAddress, String dns, String userAgent) {
        boolean checkSpidersIP = true;
        if (checkSpidersIP) {
            if (isSpiderByIP(ipAddress)) {
                //log.info("SPIDER-IP(M): " + ipAddress);
                return true;
            }
        }

        boolean checkSpiderDNS = true;
        if(checkSpiderDNS) {
            //If no DNS provided, we'll determine it from the IP address
            if(dns == null) {
                //try {
                //    dns = DnsLookup.reverseDns(ipAddress).toLowerCase();
                //} catch (IOException e) {
                //    log.info("Unable to lookup DNS for IP address: " + ipAddress + " -- ERROR: " + e.getMessage());
                //}
            }

            if(isSpiderByDomainNameRegex(dns)) {
                return true;
            }
        }

        if(isSpiderByUserAgent(userAgent)) {
            return true;
        }

        //If no match from the above, then its not detected as a spider.
        return false;
    }

    public static boolean isSpiderByUserAgent(String userAgent) {
        /*
         * 2) if the user-agent header is empty - DISABLED BY DEFAULT -
         */
        boolean checkSpidersEmptyAgent = false;
        if (checkSpidersEmptyAgent) {
            if (userAgent == null || userAgent.length() == 0) {
                log.info("spider.agentempty");
                return true;
            }
        }
        /*
         * 3) if the user-agent corresponds to one of the regexes at http://www.projectcounter.org/r4/COUNTER_robot_txt_list_Jan_2011.txt
         */
        boolean checkSpidersTxt = true;
        if (checkSpidersTxt) {

            if (userAgent != null && !userAgent.equals("")) {
                return isSpiderByUserAgentRegex(userAgent);
            }
        }

        return false;
    }

    static Integer uaLookups = 0;
    static Integer uaPosWithCacheHits = 0;
    static Integer uaPosWithCacheMiss = 0;

    /**
     * Checks the user-agent string vs a set of known regexes from spiders
     * A second Set is kept for fast-matching.
     * If a user-agent is matched once, it is added to this set with "known agents".
     * If this user-agent comes back later, we can do a quick lookup in this set,
     * instead of having to loop over the entire set with regexes again.
     *
     * @param userAgent String
     * @return true if the user-agent matches a regex
     */
    public static boolean isSpiderByUserAgentRegex(String userAgent) {
        uaLookups++;
        if(userAgentSpidersMatched.getIfPresent(userAgent) != null) {
            uaPosWithCacheHits++;
            //cache-hit
            //log.info("SPIDER-UA(M): " + userAgent);
            return true;
        } else {
            //cache-miss

            if (userAgentSpidersRegex == null) {
                loadUserAgentSpiders();
            }

            if (userAgentSpidersRegex != null) {
                Object[] regexArray = userAgentSpidersRegex.toArray();
                for (Object regex : regexArray) {
                    Matcher matcher = ((Pattern) regex).matcher(userAgent);
                    if (matcher.find()) {
                        userAgentSpidersMatched.put(userAgent, userAgent);
                        uaPosWithCacheMiss++;

                        //TODO Might also want to make a cache of non-bots to expedite misses.
                        log.info("SPIDER-UA: " + userAgent);
                        return true;
                    }
                }
            }
            return false;
        }
    }

    static Integer dnsLookups = 0;
    static Integer dnsPosWithCacheHits = 0;
    static Integer dnsPosWithCacheMiss = 0;

    //TODO DRY up this code, its very similar to isSpiderByUserAgent
    public static boolean isSpiderByDomainNameRegex(String domainName) {
        dnsLookups++;
        if (domainNameSpiderMatched.getIfPresent(domainName) != null) {
            //cache-hit
            dnsPosWithCacheHits++;
            //log.info("SPIDER-DNS(M): " + domainName);
            return true;
        } else {
            if (domainNameSpidersRegex == null) {
                loadDomainNameSpiders();
            }

            if (domainNameSpidersRegex != null) {
                Object[] regexArray = domainNameSpidersRegex.toArray();
                for (Object regex : regexArray) {
                    Matcher matcher = ((Pattern) regex).matcher(domainName);
                    if (matcher.find()) {
                        domainNameSpiderMatched.put(domainName, domainName);
                        log.info("SPIDER-DNS: " + domainName);
                        dnsPosWithCacheMiss++;
                        return true;
                    }
                }
            }
            return false;
        }
    }

    /**
     * Populate static Set spidersRegex from local txt file.
     * Original file downloaded from http://www.projectcounter.org/r4/COUNTER_robot_txt_list_Jan_2011.txt during build
     */
    public static void loadUserAgentSpiders() {
        userAgentSpidersRegex = new HashSet<Pattern>();

        String spidersTxt = "/dspace/config/Spiders-UserAgent.txt";
        DataInputStream in = null;
        try {
            FileInputStream fstream = new FileInputStream(spidersTxt);
            in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            while ((strLine = br.readLine()) != null) {
                userAgentSpidersRegex.add(Pattern.compile(strLine, Pattern.CASE_INSENSITIVE));
            }
            log.info("Loaded Spider Regex file: " + spidersTxt);
        } catch (FileNotFoundException e) {
            log.info("File with spiders regex not found @ " + spidersTxt);
        } catch (IOException e) {
            log.info("Could not read from file " + spidersTxt);
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException e) {
                log.info("Could not close file " + spidersTxt);
            }
        }
    }

    /**
     * Populate static Set spidersRegex from local txt file.
     */
    public static void loadDomainNameSpiders() {
        domainNameSpidersRegex = new HashSet<Pattern>();

        String spidersTxt = "/dspace/config/Spiders-DomainName.txt";
        DataInputStream in = null;
        try {
            FileInputStream fstream = new FileInputStream(spidersTxt);
            in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            while ((strLine = br.readLine()) != null) {
                domainNameSpidersRegex.add(Pattern.compile(strLine, Pattern.CASE_INSENSITIVE));
            }
            log.info("Loaded Spider Regex file: " + spidersTxt);
        } catch (FileNotFoundException e) {
            log.info("File with spiders regex not found @ " + spidersTxt);
        } catch (IOException e) {
            log.info("Could not read from file " + spidersTxt);
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException e) {
                log.info("Could not close file " + spidersTxt);
            }
        }
    }

    public static void printUACacheOutput() {
        log.info("UserAgent caching. Total:" + uaLookups + " postiveHits:" + uaPosWithCacheHits + " positiveMiss:" + uaPosWithCacheMiss);
    }

    public static void printDNSCacheOutput() {
        log.info("DNS caching. Total:" + dnsLookups + " postiveHits:" + dnsPosWithCacheHits + " positiveMiss:" + dnsPosWithCacheMiss);
    }

    public static Set<Pattern> getUserAgentSpidersRegex() {
        return userAgentSpidersRegex;
    }

    public static void setUserAgentSpidersRegex(Set<Pattern> spidersRegex) {
        SpiderDetector.userAgentSpidersRegex = spidersRegex;
    }

    //public static Set<String> getUserAgentSpidersMatched() {
    //    return userAgentSpidersMatched;
    //}

    //public static void setUserAgentSpidersMatched(Set<String> spidersMatched) {
    //    SpiderDetector.userAgentSpidersMatched = spidersMatched;
    //}
}
