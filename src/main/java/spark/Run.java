package spark;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.util.List;

public class Run {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage:  spark-submit ... jar <input_file> <output_folder>");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];

        SparkConf conf = new SparkConf()
                .setAppName("Review mining")
                .setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Review> reviews = sc.textFile(inputPath)
                .map(line -> Parse.parseCsvToReview(line));


        FileUtils.deleteQuietly(new File(outputPath));

        ReviewMining revMin = new ReviewMining(inputPath, reviews);

        //JavaPairRDD<String, Integer> words = revMin.countWords();

        List<Tuple2<Integer, String>> topTen = revMin.top10words();
        sc.parallelize(topTen).saveAsTextFile(outputPath);

    }
}
