package com.example.spark;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import com.mongodb.client.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.bson.Document;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Spark {

    public static void main(String[] args) {
        //create list of words we want to filter
        List<String> words = new ArrayList<>();
        words.add("Education");
        words.add("hot");
        words.add("Canada");
        words.add("cold");
        words.add("flu");
        words.add("snow");
        words.add("indoor");
        words.add("rain");
        words.add("ice");
        //create the file and put contents from the db within
       try {
            createfile();
       } catch (IOException e) {
            e.printStackTrace();
       }
        // set the path
        String path = "/Users/cisi/IdeaProjects/spark/src/outputData.txt";
        //create configuration
        SparkConf conf = new SparkConf().setAppName("Spark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //create JavaRDD object with textfile content within
        JavaRDD < String > rdd = sc.textFile(path);
        //split textfile so we can map it
        JavaRDD < String > rddWords = rdd.flatMap(l -> Arrays.asList(l.split(" ")));

        //map the strings and give it a 1 integer
        JavaPairRDD< String, Integer > pairs = rddWords.mapToPair(w -> new Tuple2(w, 1));
        //reduce and do aggregation
        JavaPairRDD < String, Integer > counts = pairs.reduceByKey((x, y) -> x + y);
        //filter and print the sum of the strings we want
        for (String keywords : words) {
            Function<Tuple2<String, Integer>, Boolean> filterFunction = w -> (w._1.equals(keywords));
            System.out.println(counts.filter(filterFunction));
        }


    }

    private static void createfile() throws IOException {
        String path = "./outputData.txt";
        PrintWriter out = new PrintWriter(
                new BufferedWriter(new FileWriter(path, true))
        );
        String connectionString = "mongodb://127.0.0.1:27017/?&gssapiServiceName=mongodb";
        try (MongoClient mongoClient = MongoClients.create(connectionString)) {
            //create ReuterDb database
            MongoDatabase process = mongoClient.getDatabase("ReuterDb");
            MongoDatabase process1 = mongoClient.getDatabase("ProcessedDb");

            List<String> keyword = new ArrayList<String>();
            keyword.add("weather");
            keyword.add("Canada");
            keyword.add("hockey");
            keyword.add("temperature");
            keyword.add("education");


            for (String key: keyword) {

                MongoCollection<Document> collection = process.getCollection(key);

                try (MongoCursor<Document> cur = collection.find().iterator()) {

                    while (cur.hasNext()) {
                        out.println(cur.next().toJson());


                    }


                }
            }

            MongoCollection<Document> collection1 = process1.getCollection("news");
            try (MongoCursor<Document> cur = collection1.find().iterator()) {

                while (cur.hasNext()) {
                    out.println(cur.next().toJson());


                }


            }
            out.flush();
            out.close();

        }
    }
}
