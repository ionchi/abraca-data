package spark;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaPairRDD;

import java.io.File;

public class Run {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage:  spark-submit ... jar <input_file> <output_folder>");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];

        FileUtils.deleteQuietly(new File(outputPath));

        ReviewMining revMin = new ReviewMining(inputPath);

        JavaPairRDD<String, Integer> words = revMin.countWords();

        words.saveAsTextFile(outputPath);
    }
}
