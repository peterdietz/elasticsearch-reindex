package com.pannous.es.reindex;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.rest.RestController;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
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

        String filter = "{\n" +
                "   \"query\": {\n" +
                "       \"bool\" : {\n" +
                "           \"must\" : { \n" +
                "               \"match_all\": {}\n" +
                "           }, \n" +
                "           \"must_not\" : {\n" +
                "               \"term\" : {\n" +
                "                   \"isBot\" : true\n" +
                "               }\n" +
                "           }\n" +
                "       },\n" +
                "      \"constant_score\" : {\n" +
                "        \"filter\" : {\n" +
                "            \"exists\" : { \"field\" : \"userAgent\" }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "}";


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
                int max = 500;
                int i = 0;

                for (MySearchHit h : hitsIterator) {

                    //Testing, stop at 500
                    i++;
                    if(i>max) {
                        log.info("Hit 500, stopping...");
                        break;
                    }

                    try {
                        String str = new String(h.source(), charset);
                        RewriteSearchHit newHit = new RewriteSearchHit(h.id(), h.version(), str);
                        String someField = newHit.get("userAgent");

                        if(SpiderDetector.isSpiderByUserAgent(someField)) {
                            newHit.put("isBotUA", true);
                        } else {
                            newHit.put("isBotUA", false);
                        }


                        newHit.put("roboChecked", true);

                        res.add(newHit);
                        log.info(someField);
                    } catch (UnsupportedEncodingException ex) {
                        throw new RuntimeException(ex);
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
