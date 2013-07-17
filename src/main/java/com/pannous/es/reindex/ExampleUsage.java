package com.pannous.es.reindex;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.rest.RestController;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Class to use the reindex plugin as rewrite/refeed plugin - directly from
 * java.
 *
 * @author Peter Karich
 */
public class ExampleUsage {

    private final static String charset = "UTF-8";

    private static Logger log = Logger.getLogger("test");

    public static void main(String[] args) {
        String searchHost = "1.1.1.1";
        int searchPort = 9300;
        String searchIndexName = "dspace03";
        String searchType = "stats";
        String newIndexName = "dspace03";
        String newType = "stats";
        // String filter = "{ 'term' : {'locale' : 'de'} }".replaceAll("'", "\"");
        //String filter = "{ 'query' : {'query_string' : { 'query' : 'isBot:false'} } }".replaceAll("'", "\"");

        /*String filter = "{\n" +
                "   \"query\": {\n" +
                "       \"bool\" : {\n" +
                "           \"must\" : { \n" +
                "                    \"match_all\" : { }\n" +
                "           }, \n" +
                "           \"must_not\" : [\n" +
                "                  {\n" +
                "                    \"term\" : {\n" +
                "                       \"isBot\" : true\n" +
                "                    }\n" +
                "                  },\n" +
                "                  {\n" +
                "                    \"term\" : {\n" +
                "                       \"isBotUA\" : true\n" +
                "                    }\n" +
                "                  },\n" +
                "                  {\n" +
                "                    \"term\" : {\n" +
                "                       \"roboChecked\" : false\n" +
                "                    }\n" +
                "                  }\n" +
                "           ]\n" +
                "       },\n" +
                "      \"constant_score\" : {\n" +
                "        \"filter\" : {\n" +
                "          \"and\" : [\n" +
                "              {\n" +
                "                \"exists\" : { \"field\" : \"userAgent\" }\n" +
                "              }\n" +
                "            ]\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "}"; */

        Calendar cal = Calendar.getInstance();
        //TODO set to cut-off date. June 30 2013
        Date dateEnd = cal.getTime();

        //TODO set to Tschera's start date. 2012-April-1
        cal.set(Calendar.YEAR, 2013);
        cal.set(Calendar.MONTH, Calendar.JUNE);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR_OF_DAY,0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        Date dateStart = cal.getTime();



        FilterBuilder dateRangeFilter = FilterBuilders.rangeFilter("time").from(dateStart).to(dateEnd);
        FilterBuilder filter = FilterBuilders.boolFilter()
                .must(dateRangeFilter)
                .mustNot(FilterBuilders.termFilter("roboCheckedEight", true))
                .mustNot(FilterBuilders.termFilter("isBot", true))
                .mustNot(FilterBuilders.termFilter("isBotReindexEight", true));



        String basicAuthCredentials = "base64_ifrequried=";
        boolean withVersion = false;
        final int hitsPerPage = 500;
        float waitInSeconds = 0.1f;
        // increase if you have lots of things to update
        int keepTimeInMinutes = 90;
        String cluster = "your_production_cluster_name";



        boolean local = true;
        if (local) {
            cluster = "elasticsearch";
            searchHost = "localhost";
            basicAuthCredentials = "base64_ifrequried=";
        }

        log.info("querying " + searchHost + ":" + searchPort
                + " at " + searchIndexName + " with " + basicAuthCredentials);

        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", cluster).build();
        Client client = new TransportClient(settings).
                addTransportAddress(new InetSocketTransportAddress(searchHost, searchPort));

        Settings emptySettings = ImmutableSettings.settingsBuilder().build();
        RestController contrl = new RestController(emptySettings);
        ReIndexAction action = new ReIndexAction(emptySettings, client, contrl) {
            @Override protected MySearchHits callback(MySearchHits hits) {
                SimpleList res = new SimpleList(hitsPerPage, hits.totalHits());
                Iterable<MySearchHit> hitsIterator = hits.getHits();

                for (MySearchHit h : hitsIterator) {
                    try {
                        String str = new String(h.source(), charset);
                        RewriteSearchHit newHit = new RewriteSearchHit(h.id(), h.version(), str);

                        //Detect if this hit was from a robot, based on available IP / DNS / UserAgent
                        String ip           = newHit.get("ip");
                        String dns          = newHit.get("dns");
                        String userAgent    = newHit.get("userAgent");

                        if(SpiderDetector.isSpiderByIPOrDomainNameOrUserAgent(ip, dns, userAgent)) {
                            newHit.put("isBotReindexEight", true);
                        } else {
                            newHit.put("isBotReindexEight", false);
                        }


                        newHit.put("roboCheckedEight", true);

                        res.add(newHit);
                    } catch (Exception e) {
                        if(e.getMessage().contains("Duplicate key")) {
                            log.info("Invalid hit, based on UA, skipping?");
                        } else {
                            throw new RuntimeException(e);
                        }
                    }
                }
                return res;
            }
        };
        // first query, further scroll-queries in reindex!
        SearchRequestBuilder srb = action.createScrollSearch(searchIndexName, searchType, filter,
                hitsPerPage, withVersion, keepTimeInMinutes);
        SearchResponse sr = srb.execute().actionGet();
        MySearchResponse rsp = new MySearchResponseES(client, sr, keepTimeInMinutes);

        // now feed and call callback
        action.reindex(rsp, newIndexName, newType, withVersion, waitInSeconds);

        SpiderDetector.printDNSCacheOutput();
        SpiderDetector.printUACacheOutput();

        client.close();
    }

    public static class SimpleList implements MySearchHits {

        long totalHits;
        List<MySearchHit> hits;

        public SimpleList(int size, long total) {
            hits = new ArrayList<MySearchHit>(size);
            totalHits = total;
        }

        public void add(MySearchHit hit) {
            hits.add(hit);
        }

        @Override public Iterable<MySearchHit> getHits() {
            return hits;
        }

        @Override
        public long totalHits() {
            return totalHits;
        }
    }

    public static class RewriteSearchHit implements MySearchHit {

        String id;
        long version;
        JSONObject json;

        public RewriteSearchHit(String id, long version, String jsonStr) {
            this.id = id;
            this.version = version;
            try {
                json = new JSONObject(jsonStr);
            } catch (JSONException ex) {
                //java.lang.RuntimeException: org.json.JSONException: Duplicate key "userAgent"
                log.log(Level.WARNING, jsonStr);
                throw new RuntimeException(ex);
            }
        }

        public String get(String key) {
            try {
                if (!json.has(key))
                    return "";
                String val = json.getString(key);
                if (val == null)
                    return "";
                return val;
            } catch (JSONException ex) {
                throw new RuntimeException(ex);
            }
        }

        public JSONObject put(String key, Object obj) {
            try {
                return json.put(key, obj);
            } catch (JSONException ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override public String id() {
            return id;
        }

        @Override public long version() {
            return version;
        }

        @Override public byte[] source() {
            try {
                return json.toString().getBytes(charset);
            } catch (UnsupportedEncodingException ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}
