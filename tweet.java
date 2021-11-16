Problem 1
import com.mongodb.MongoCommandException;
import com.mongodb.client.*;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Scanner;
import org.apache.http.HttpEntity;
import org.bson.Document;
import org.bson.types.ObjectId;


/*
     * Sample code to demonstrate the use of the Recent search endpoint
     * */
    public class tweet {

    // To set your enviornment variables in your terminal run the following line:
    // export 'BEARER_TOKEN'='<your_bearer_token>'

    public static void main(String args[]) throws IOException, URISyntaxException {





        String bearerToken = "AAAAAAAAAAAAAAAAAAAAAIJqVgEAAAAA1%2F%2Fw%2BMDzt%2FAZIEDPbYhmIVTlZ3A%3DKDTagupw2ZrnGGzbGiH35bYFDIRSBXKq59Tn2SEI6O8obtCeRc";


        List<String> searchTerm = Arrays.asList("weather", "hockey", "Canada", "Temperature", "Education");

        String fields = "created_at,author_id,source";

        for (String search : searchTerm) {

            String response = search(search, bearerToken, fields);
            String doc = "\\{(.*?)}";
            String next = "(?<=\\\"next_token\\\":).*";
            Pattern f = Pattern.compile(doc);
            Matcher m1 = f.matcher(response);
            Pattern n = Pattern.compile(next);

            List<String> trext = new ArrayList<String>();


            while (m1.find() ) {

                for (int a = 1; a <= m1.groupCount(); a++) {
                    //System.out.println((m1.group(a)));
                   trext.add(m1.group(a));
                }

            }
            Matcher m2 = n.matcher(trext.get(trext.size()-1));
            System.out.println(trext.get(trext.size()-1));
            while (m2.find()){

                String token = m2.group(0).replaceAll("\"","");
                System.out.println(token);
                for (int i = 0; i < 3; i++) {
                        System.out.println("inside if");
                        String response1 = search(search, bearerToken, fields, token);
                        Matcher m3 = f.matcher(response1);
                        while (m3.find()) {
                            System.out.println("inside m3");
                            for (int a = 1; a <= m3.groupCount(); a++) {
                                trext.add(m3.group(a));
                                System.out.println("inside pagination");
                            }
                        }
                    String bext = "(?<=\\\"next_token\\\":).*";
                    Pattern m = Pattern.compile(bext);

                    Matcher m4 = m.matcher(trext.get(trext.size()-1));
                        System.out.println(trext.get(trext.size()-1));
                        System.out.println(m4.find());
                        token = m4.group(0).replaceAll("\"","");

                }
            }

           mongo(nextRegex(trext),search);
            List<Document> cleantweet = List.copyOf(nextRegex(trext));
            regex(cleantweet, search);

        }
    }

    private static String search(String search, String bearerToken, String fields, String group) throws URISyntaxException, IOException {
        String searchResponse = null;
        Logger log = Logger.getLogger(tweet.class.getName());

        HttpClient httpClient = HttpClients.custom()
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setCookieSpec(CookieSpecs.STANDARD).build())
                .build();
        System.out.println("inside overload");
        URIBuilder uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/recent");
        ArrayList<NameValuePair> queryParameters;
        queryParameters = new ArrayList<>();
        queryParameters.add(new BasicNameValuePair("query", search));
        queryParameters.add(new BasicNameValuePair("tweet.fields", fields));
        queryParameters.add(new BasicNameValuePair("max_results", "100"));
        queryParameters.add(new BasicNameValuePair("next_token", group));
        uriBuilder.addParameters(queryParameters);


        HttpGet httpGet = new HttpGet(uriBuilder.build());

        httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken));
        httpGet.setHeader("Content-Type", "application/json");

        HttpResponse response = httpClient.execute(httpGet);
        HttpEntity entity = response.getEntity();
        if (null != entity) {
            searchResponse = EntityUtils.toString(entity, "UTF-8");
        }


        return searchResponse;

    }

    //regex to get the tweet information from the tweets stored in the string list array
    private static List<Document> nextRegex(List<String> text) {
        String aut = "\"author_id\":\"(.*?)\"";
        String crea = "\"created_at\":\"(.*?)\"";
        String sour = "\"source\":\"(.*?)\"";
        String tex = "\"text\":\"(.*?)\"";
        String id = "\"id\":\"(.*?)\"";
        Pattern g = Pattern.compile(aut);
        Pattern h = Pattern.compile(crea);
        Pattern i = Pattern.compile(sour);
        Pattern j = Pattern.compile(tex);
        Pattern k = Pattern.compile(id);
        System.out.println(text);
        int counter = 0;
        List<Document> tweets = new ArrayList<Document>();

        //loop to iterate through each line and match the required texts
        for (String extract : text) {
            Matcher m2 = g.matcher(extract);
            Matcher m3 = h.matcher(extract);
            Matcher m4 = i.matcher(extract);
            Matcher m5 = j.matcher(extract);
            Matcher m6 = k.matcher(extract);
            if (m2.find() && m3.find() && m4.find() && m5.find() && m6.find()) {


                Document tweet = new Document("_id", new ObjectId());
                tweet.append("tweet_id", counter)
                        .append("author_id", m2.group(1))
                        .append("created_at", m3.group(1))
                        .append("source", m4.group(1))
                        .append("text", m5.group(1))
                        .append("id", m6.group(1)) ;

                counter++;
                tweets.add(tweet);
                System.out.println("inside document creator");

            }

        }


        return tweets;
    }

    /*
     * This method calls the recent search endpoint with a the search term passed to it as a query parameter
     * */
    private static String search(String searchString, String bearerToken, String fields) throws IOException, URISyntaxException {
        String searchResponse = null;
        Logger log = Logger.getLogger(tweet.class.getName());

        HttpClient httpClient = HttpClients.custom()
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setCookieSpec(CookieSpecs.STANDARD).build())
                .build();

        URIBuilder uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/recent");
        ArrayList<NameValuePair> queryParameters;
        queryParameters = new ArrayList<>();
        queryParameters.add(new BasicNameValuePair("query", searchString));
        queryParameters.add(new BasicNameValuePair("tweet.fields", fields));
        queryParameters.add(new BasicNameValuePair("max_results", "100"));
        uriBuilder.addParameters(queryParameters);
        log.log(Level.INFO, "uri:", uriBuilder.toString());

        HttpGet httpGet = new HttpGet(uriBuilder.build());
        log.log(Level.INFO, "httpget:", httpGet.toString());
        httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken));
        httpGet.setHeader("Content-Type", "application/json");

        HttpResponse response = httpClient.execute(httpGet);
        HttpEntity entity = response.getEntity();
        if (null != entity) {
            searchResponse = EntityUtils.toString(entity, "UTF-8");
        }


        return searchResponse;
    }

    private static void mongo(List<Document> tweets, String keyword) {

        //create connection to monogodb
        String connectionString = "mongodb://127.0.0.1:27017/?&gssapiServiceName=mongodb";
        try (MongoClient mongoClient = MongoClients.create(connectionString)) {
            //create RawDb database
            MongoDatabase process = mongoClient.getDatabase("RawDb");

            try {
                process.createCollection(keyword);
                System.out.println("Collection created successfully");
            } catch (MongoCommandException e) {
                process.getCollection(keyword).drop();
            }
            //create news collection and document to store the news articles and metadata
            MongoCollection<Document> procTweet = process.getCollection(keyword);
            procTweet.insertMany(tweets);
            System.out.println("just finished putting all rawdb");

        }
    }

   /* private static void regex(String keyword) {

        String connectionString = "mongodb://127.0.0.1:27017/?gssapiServiceName=mongodb";
        try (MongoClient mongoClient = MongoClients.create(connectionString)) {
            MongoDatabase process = mongoClient.getDatabase("RawDb");
            MongoCollection<Document> proc = process.getCollection(keyword);
            FindIterable<Document> tweets = proc.find();
            for (Document next : tweets) {
                regex(next, keyword);
            }
        } catch (MongoCommandException e) {

        }


    }*/

    private static void regex(List<Document> tweet, String keyword) {
        String re = "\\w+:\\/\\/.*?(?=\\s)";
        String be = "[^a-zA-Z0-9_,{} =@:\\“. \\…\\[\\]\\\"\\-]";
        String arab = "[\\u0600-\\u06FF]+";
        String de = "[\\ud83c\\udc00-\\ud83c\\udfff]|[\\ud83d\\udc00-\\ud83d\\udfff]|[\\u2600-\\u27ff]";
        String ve = "";
        List<Document> cleanTweets = new ArrayList<Document>();
        for (Document extract : tweet) {
            String unclean= extract.get("text").toString();
            Pattern p = Pattern.compile(re);
            Matcher m = p.matcher(unclean);
            String clean = m.replaceAll(ve);
            Pattern p1 = Pattern.compile(arab);
            Matcher m1 = p1.matcher(clean);
            String clean1 = m1.replaceAll(ve);
            Pattern p2 = Pattern.compile(de);
            Matcher m2 = p2.matcher(clean1);
            String clean2 = m2.replaceAll(ve);
            Pattern p3 = Pattern.compile(be);
            Matcher m3 = p3.matcher(clean2);
            String clean3 = m3.replaceAll(ve);

            extract.put("text", clean3);
            cleanTweets.add(extract);

        }
        System.out.println("clean tweets");
        processedMongo(cleanTweets, keyword);
    }

    private static void processedMongo(List<Document> clean2, String keyword) {
        String connectionString = "mongodb://127.0.0.1:27017/?gssapiServiceName=mongodb";
        try (MongoClient mongoClient = MongoClients.create(connectionString)) {
            MongoDatabase process = mongoClient.getDatabase("ProcessedDb");

            try {
                process.createCollection(keyword);
                System.out.println("Collection created successfully");
            } catch (MongoCommandException e) {
                process.getCollection(keyword).drop();
            }

            MongoCollection<Document> procTweet = process.getCollection(keyword);

            procTweet.insertMany(clean2);
            System.out.println("inserted you good");
        }
    }
}

