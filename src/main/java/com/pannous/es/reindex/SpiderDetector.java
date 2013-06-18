/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree and available online at
 *
 * http://www.dspace.org/license/
 */
package com.pannous.es.reindex;

import java.io.*;
import java.util.HashSet;
import java.util.Set;
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

    private static Set<Pattern> spidersRegex = null;
    private static Set<String> spidersMatched = null;

    private static Logger log = Logger.getLogger("test");


    public static boolean isSpiderByUserAgent(String userAgent) {
        /*
         * 2) if the user-agent header is empty - DISABLED BY DEFAULT -
         */
        //if (userAgent == null || userAgent.length() == 0) {
        //    log.debug("spider.agentempty");
        //    return true;
        //}

        /*
         * 3) if the user-agent corresponds to one of the regexes at http://www.projectcounter.org/r4/COUNTER_robot_txt_list_Jan_2011.txt
         */
        if (userAgent != null && !userAgent.equals("")) {
            return isSpiderRegex(userAgent);
        }

        return false;
    }

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
    public static boolean isSpiderRegex(String userAgent) {
        if (spidersMatched != null && spidersMatched.contains(userAgent)) {
            log.info("spider.agentregex");
            return true;
        } else {
            if (spidersRegex == null) {
                loadSpiderRegexFromFile();
            }

            if (spidersRegex != null) {
                Object[] regexArray = spidersRegex.toArray();
                for (Object regex : regexArray) {
                    Matcher matcher = ((Pattern) regex).matcher(userAgent);
                    if (matcher.find()) {
                        if (spidersMatched == null) {
                            spidersMatched = new HashSet<String>();
                        }
                        if (spidersMatched.size() >= 100) {
                            spidersMatched.clear();
                        }
                        spidersMatched.add(userAgent);
                        log.info("spider.agentregex");
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
    public static void loadSpiderRegexFromFile() {
        spidersRegex = new HashSet<Pattern>();
        String spidersTxt = "/dspace/config/SpidersJan2011.txt";
        DataInputStream in = null;
        try {
            FileInputStream fstream = new FileInputStream(spidersTxt);
            in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            while ((strLine = br.readLine()) != null) {
                spidersRegex.add(Pattern.compile(strLine, Pattern.CASE_INSENSITIVE));
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

    public static Set<Pattern> getSpidersRegex() {
        return spidersRegex;
    }

    public static void setSpidersRegex(Set<Pattern> spidersRegex) {
        SpiderDetector.spidersRegex = spidersRegex;
    }

    public static Set<String> getSpidersMatched() {
        return spidersMatched;
    }

    public static void setSpidersMatched(Set<String> spidersMatched) {
        SpiderDetector.spidersMatched = spidersMatched;
    }
}
