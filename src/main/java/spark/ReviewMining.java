package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class ReviewMining {

    private String pathToFile;
    private JavaRDD<Review> reviews;

    public ReviewMining(String file, JavaRDD<Review> reviews){
        this.reviews = reviews;
        this.pathToFile = file;
    }

    public JavaRDD<Review> loadData() {
        // create spark configuration and spark context
        SparkConf conf = new SparkConf()
                .setAppName("Review mining")
                .setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Review> reviews = sc.textFile(pathToFile)
                .map(line -> Parse.parseCsvToReview(line));
        return reviews;
    }

    // dividere il summary in lista di parole
    public JavaRDD<String> wordsInSummary() {
        //JavaRDD<Review> reviews = loadData();
        JavaRDD<String> words =
                reviews.flatMap(review -> Arrays.asList(review.getSummary().split(" ")).iterator())
                        .filter(word -> word.length() > 1);
        System.out.println("review.count() " + reviews.count());
        return words;
    }

    public JavaPairRDD<String, Integer> countWords() {
        JavaRDD<String> words = wordsInSummary();
        JavaPairRDD<String, Integer> wordCount =
                words.mapToPair(word -> new Tuple2<>(word, 1))
                        .reduceByKey((x, y) -> x + y);
        return wordCount;
    }

    public List<Tuple2<Integer, String>> top10words() {
        JavaPairRDD<String, Integer> counts = countWords();
        List<Tuple2<Integer, String>> mostMentioned =
                counts.mapToPair(pair -> new Tuple2<>(pair._2(), pair._1()))
                        .sortByKey(false)
                        .take(40);
        return mostMentioned;
    }
}
