import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.mongodb.MongoCommandException;
import com.mongodb.client.*;
import org.bson.Document;
import org.bson.types.ObjectId;


public class reuters {



    public static void main(String[] args) {
        //store path information
        String path = "reut2-014.sgm";
        String path1 = "reut2-009.sgm";
        //read file into arraylist to enable processing
        List<String> reuter = readfile(path, path1);

        //extract news items and add to mongodb database
        Extract(reuter);

    }

    private static void Extract(List<String> reuter) {
        //set regex parameters
        String date = "<DATE>(.*?)<\\/DATE>";
        String title = "<TITLE>(.*?)<\\/TITLE>";
        String text = "<BODY>(.*?)<\\/BODY>";
        Pattern f = Pattern.compile(date);
        Pattern s = Pattern.compile(title);
        Pattern t = Pattern.compile(text);
        int counter = 0;
        List<Document> news = new ArrayList<Document>();
        //loop to iterate through each line and match the required texts
        for (String extract : reuter) {
            Matcher m1 = f.matcher(extract);
            Matcher m2 = s.matcher(extract);
            Matcher m3 = t.matcher(extract);
            if (m1.find() && m2.find() && m3.find()) {
                Document newss = new Document("_id" , new ObjectId());
                newss.append("news_id" , counter)
                        .append("Title", m1.group(1))
                        .append("News", m2.group(1))
                        .append("date", m3.group(1));
               counter++;
               news.add(newss);
            }


        }
        Mongo(news);

    }

    private static void Mongo(List<Document> news) {


        //create connection to monogodb
        String connectionString = "mongodb://127.0.0.1:27017/?&gssapiServiceName=mongodb";
        try (MongoClient mongoClient = MongoClients.create(connectionString)) {
            //create ReuterDb database
            MongoDatabase process = mongoClient.getDatabase("ReuterDb");

            try {
                process.createCollection("news");
                System.out.println("Collection created successfully");
            } catch (MongoCommandException e) {
                process.getCollection("news").drop();
            }
            //create news collection and document to store the news articles and metadata
            MongoCollection<Document> procTweet = process.getCollection("news");
            procTweet.insertMany(news);

        }

    }


    private static List<String> readfile(String path, String path2) {

        try {
            //read file text from both files
            List<String> result = Files.readAllLines(Paths.get(path));
            List<String> result1 = Files.readAllLines(Paths.get(path2));
            //store all in one arraylist
            result.addAll(result1);
            List<String> line = new ArrayList<String>();
            String reut = "";
            //loop to go through each index storing all texts between reuter tags as a line to simplify processing
            for (String reuter : result) {

                reut= reut + reuter;

                if (reuter.equals("</REUTERS>")){

                    line.add(reut);
                    reut ="";

                }


            }

            return line;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
    }


