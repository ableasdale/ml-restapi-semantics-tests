import nu.xom.*;
import org.asynchttpclient.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.net.URLEncoder.encode;
import static org.asynchttpclient.Dsl.asyncHttpClient;

public class Main {

    public static final String TURTLE_FILE = "src/main/resources/humord.ttl";
    private static final int TRIES = 1000;
    private static final int EXPECTED_RESULT = 175578;

    private static Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    // URIs and Query Components
    private static String HOST = "localhost";
    private static String BASE = "http://"+HOST+":8000";
    private static String TX_URL = BASE + "/v1/transactions";
    private static String PUT_BASEURL = "http://localhost:8000/v1/graphs?default";
    private static String BASEURL = "http://localhost:8000/v1/graphs/sparql?query=";
    private static String BASEURL_XQY = "http://localhost:8000/v1/eval";
    private static String SELECT = "select (count(*) as ?n) where {graph <http://marklogic.com/semantics#default-graph> {?s ?p ?o} }";
    private static String ORDERBY = "  order by ?s ?p ?o";
    private static String SELECT_ORDERBY = SELECT + ORDERBY;
    private static String SEM_SPARQL = "sem:sparql(\"";
    private static String SEM_SPARQL_OPTS = "\", (), (\"optimize=0\"))";
    private static String CONSTRUCT = "construct { ?s ?p ?o } where {graph <http://marklogic.com/semantics#default-graph> {?s ?p ?o} }";
    private static String CONSTRUCT_ORDERBY = CONSTRUCT + ORDERBY;
    private static String XQY_SELECT = SEM_SPARQL + SELECT + SEM_SPARQL_OPTS;
    private static String XQY_SELECT_ORDERBY = SEM_SPARQL + SELECT_ORDERBY + SEM_SPARQL_OPTS;
    private static String XQY_CONSTRUCT = SEM_SPARQL + CONSTRUCT + SEM_SPARQL_OPTS;
    private static String XQY_CONSTRUCT_ORDERBY = SEM_SPARQL + CONSTRUCT_ORDERBY + SEM_SPARQL_OPTS;

    private static String buildQuery
            (String baseUrl, String queryString) {
        StringBuilder sb = new StringBuilder();
        try {
            sb.append(baseUrl).append(encode(queryString, StandardCharsets.UTF_8.toString()));
        } catch (UnsupportedEncodingException e) {
            LOG.error("Problem creating Query: ", e);
        }
        return sb.toString();
    }

    private static boolean runXqyConstructTest(AsyncHttpClient asyncHttpClient, Realm realm, String query) {
        boolean pass = false;
        Request req = asyncHttpClient.preparePost(BASEURL_XQY)
                .setHeader("Content-type", "application/x-www-form-urlencoded")
                .setBody("xquery=" + query)
                .setRealm(realm)
                .build();
        try {
            ListenableFuture<Response> whenResponse = asyncHttpClient.executeRequest(req);
            Response response = whenResponse.get();
            String sRes = response.getResponseBody();
            int lines = 0;
            for (String s : sRes.split(System.getProperty("line.separator"))) {
                if (s.startsWith("sem:triple")) {
                    lines += 1;
                }
            }
            pass = lines == EXPECTED_RESULT;
        } catch (ExecutionException | InterruptedException e) {
            LOG.error("Exception caught:", e);
        }
        return pass;
    }

    private static boolean runXqySelectTest(AsyncHttpClient asyncHttpClient, Realm realm, String query) {
        boolean pass = false;
        Request req = asyncHttpClient.preparePost(BASEURL_XQY)
                .setHeader("Content-type", "application/x-www-form-urlencoded")
                .setBody("xquery=" + query)
                .setRealm(realm)
                .build();
        try {
            ListenableFuture<Response> whenResponse = asyncHttpClient.executeRequest(req);
            Response response = whenResponse.get();
            String sRes = response.getResponseBody();
            // LOG.info(sRes);
            // TODO - I may be missing something here but it seems to be the case that /v1/eval contains an unusual response (multipart) style and can't be changed, so I'm getting a subsequence for now to grab the result from the HTTP response and parse as an Integer.
            // TODO - fix this so it gets the line containing the JSON sequence and gets the value out properly!
            pass = Integer.parseInt((String) sRes.subSequence(sRes.indexOf("\"n\":") + 4, sRes.indexOf("\"n\":") + 10)) == EXPECTED_RESULT;
            //pass = response.getResponseBody().split(System.getProperty("line.separator")).length == EXPECTED_RESULT;
            //LOG.info("lines"+response.getResponseBody().split(System.getProperty("line.separator")).length);
        } catch (ExecutionException | InterruptedException e) {
            LOG.error("Exception caught:", e);
        }
        return pass;
    }

    private static boolean runConstructTest(AsyncHttpClient asyncHttpClient, Realm realm, String query) {
        boolean pass = false;
        Future<Response> whenResponse = asyncHttpClient.prepareGet(buildQuery(BASEURL, query))
                .setHeader("Accept", "application/n-triples")
                .setRealm(realm)
                .execute();
        try {
            Response response = whenResponse.get();
            pass = response.getResponseBody().split(System.getProperty("line.separator")).length == EXPECTED_RESULT;
            //LOG.info("lines"+response.getResponseBody().split(System.getProperty("line.separator")).length);
        } catch (ExecutionException | InterruptedException e) {
            LOG.error("Exception caught:", e);
        }
        return pass;
    }


    private static boolean runSelectTest(AsyncHttpClient asyncHttpClient, Realm realm, String query) {
        boolean pass = false;
        Future<Response> whenResponse = asyncHttpClient.prepareGet(buildQuery(BASEURL, query))
                .setHeader("Accept", "application/sparql-results+xml")
                .setRealm(realm)
                .execute();
        try {
            Response response = whenResponse.get();
            Builder parser = new Builder();
            Document doc = parser.build(response.getResponseBody(), null);//"http://www.w3.org/2005/sparql-results#"
            XPathContext xc = XPathContext.makeNamespaceContext(doc.getRootElement());
            xc.addNamespace("sp", "http://www.w3.org/2005/sparql-results#");
            /* too lazy to figure out how to deal with namespaces so traversing the result doc using primitive XPath.
            //Nodes titles = doc.query("/*[1]/*[2]/*[1]/*[1]/*[1]"); */
            Nodes titles = doc.query("//sp:result", xc);
            pass = Integer.parseInt(titles.get(0).getValue()) == EXPECTED_RESULT;
        } catch (ExecutionException | IOException | InterruptedException | ParsingException e) {
            LOG.error("Exception caught:", e);
        }
        return pass;
    }

    private static void deleteGraph(AsyncHttpClient asyncHttpClient, Realm realm) {

        Request req = asyncHttpClient.prepareDelete(PUT_BASEURL)
                .setRealm(realm)
                .build();
        try {
            ListenableFuture<Response> whenResponse = asyncHttpClient.executeRequest(req);
            Response response = whenResponse.get();
            if(response.getStatusCode() != 204){
                LOG.warn("Unexpected response code from DELETE: "+response.getStatusCode());
            }
        } catch (ExecutionException | InterruptedException e) {
            LOG.error("Exception caught:", e);
        }
    }

    private static void putGraph(AsyncHttpClient asyncHttpClient, Realm realm) {
        Request req = asyncHttpClient.preparePut(PUT_BASEURL)
                .setHeader("Content-type", "text/turtle")
                .setBody(new File(TURTLE_FILE))
                .setRealm(realm)
                .build();
        try {
            ListenableFuture<Response> whenResponse = asyncHttpClient.executeRequest(req);
            Response response = whenResponse.get();
            if(response.getStatusCode() != 204){
                LOG.warn("Unexpected response code from PUT: "+response.getStatusCode());
            }
        } catch (ExecutionException | InterruptedException e) {
            LOG.error("Exception caught:", e);
        }
    }

    private static String startTransaction(AsyncHttpClient asyncHttpClient, Realm realm) {
        String txId = null;
        Request req = asyncHttpClient.preparePost(TX_URL)
                .setHeader("Content-type", "text/plain")
                .setRealm(realm)
                .build();
        try {
            ListenableFuture<Response> whenResponse = asyncHttpClient.executeRequest(req);
            Response response = whenResponse.get();
            txId = response.getHeader("Location");
        } catch (ExecutionException | InterruptedException e) {
            LOG.error("Exception caught:", e);
        }
        return txId;
    }

    private static void commitTransaction(AsyncHttpClient asyncHttpClient, Realm realm, String txid) {
        Request req = asyncHttpClient.preparePost(BASE+txid+"?result=commit")
                .setHeader("Content-type", "text/plain")
                .setRealm(realm)
                .build();
        try {
            ListenableFuture<Response> whenResponse = asyncHttpClient.executeRequest(req);
            Response response = whenResponse.get();
            if(response.getStatusCode() != 204){
                LOG.warn("Unexpected response code from commit: "+response.getStatusCode());
                //LOG.info("Tx: commit "+response.getStatusCode());
            }
        } catch (ExecutionException | InterruptedException e) {
            LOG.error("Exception caught:", e);
        }
    }

    private static void logIfFalse(boolean result) {
        if (!result) {
            LOG.error("Test failed!");
        }
    }

    public static void main(String[] args) {

        // Initial setup
        Realm realm = new Realm.Builder("admin", "admin")
                .setScheme(Realm.AuthScheme.DIGEST)
                .build();

        AsyncHttpClient asyncHttpClient = asyncHttpClient();

        for (int i = 0; i < TRIES; i++) {
            String txId = startTransaction(asyncHttpClient, realm);
            LOG.info("Run: " + i  + " (txid: "+txId+")");
            //deleteGraph(asyncHttpClient, realm);
            putGraph(asyncHttpClient, realm);
            commitTransaction(asyncHttpClient, realm, txId);

            logIfFalse(runSelectTest(asyncHttpClient, realm, SELECT));
            logIfFalse(runSelectTest(asyncHttpClient, realm, SELECT_ORDERBY));

            logIfFalse(runConstructTest(asyncHttpClient, realm, CONSTRUCT));
            logIfFalse(runConstructTest(asyncHttpClient, realm, CONSTRUCT_ORDERBY));

            logIfFalse(runXqySelectTest(asyncHttpClient, realm, XQY_SELECT));
            logIfFalse(runXqySelectTest(asyncHttpClient, realm, XQY_SELECT_ORDERBY));

            logIfFalse(runXqyConstructTest(asyncHttpClient, realm, XQY_CONSTRUCT));
            logIfFalse(runXqyConstructTest(asyncHttpClient, realm, XQY_CONSTRUCT_ORDERBY));
        }

        try {
            asyncHttpClient.close();
        } catch (IOException e) {
            LOG.error("Exception caught:", e);
        }
    }
}
