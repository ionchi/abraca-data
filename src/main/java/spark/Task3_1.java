package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import utils.Utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static utils.AmazonFFRConstants.*;

public class Task3_1 implements Serializable {
    public static void main(String[] args) {
        String inputPath = args[0];
        String outputPath = args[1];
        long start = System.currentTimeMillis();
        new Task3_1().run(inputPath, outputPath);
        long elapsed = System.currentTimeMillis() - start;
        System.out.println("TEMPO TRASCORSO = " + elapsed / 1000.0 + " secondi.");
    }

    private void run(String inputPath, String outputPath) {
        SparkConf conf = new SparkConf().setAppName(this.getClass().getSimpleName());
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> input = jsc.textFile(inputPath).cache();

        input.map(row -> this.splitRow(row))
                .flatMap(x -> Arrays.asList(x._2).listIterator()) // divide il summary in parole (che sono anno+parola)
                .filter(x -> !x.equals("")) // elimina le parole vuote
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((x, y) -> x + y)
                .mapToPair(pair -> new Tuple2<>(pair._2(), pair._1()))
                .sortByKey(false) // ordinamento delle parole in base alle occorrenze
                .mapToPair(x -> changeKey(x._2,x._1)) // cambio key in anno
                .groupByKey() // group by anno
                .mapToPair(x -> topNWords(x._1,x._2)) // selezione delle prime 10 parole per anno (già ordinate prima)
                .sortByKey(true) // ordinamento opzionale in base agli anni
                .saveAsTextFile(outputPath);

        jsc.stop();
        jsc.close();
    }

    // divisione iniziale di ogni riga in (anno, array[anno+parola])
    private Tuple2<String, String[]> splitRow(String row) {
        String[] fields = row.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        if (fields.length != 10) {
            return new Tuple2<>("Errore", null);
        }
        String summary = fields[SUMMARY].toLowerCase();
        String[] words = summary.split("\\W+");
        long time = Long.parseLong(fields[TIME]);
        String year = Utils.unix2StringYear(time);
        for(int i = 0;i<words.length;i++) {
            if(!words[i].equals("")){
                words[i] = year + " " + words[i];
            }
            else
                words[i] = "";
        }
        return new Tuple2<>(year, words);
    }

    // cambia le righe da (anno+parola, numero occorrenze) -> (anno, parola+occorrenze)
    private Tuple2<Integer, String> changeKey(String yearWord, Integer count) {
        String[] key = yearWord.split(" ");
        String newValue = key[1] + " - " + count;
        int newKey = Integer.parseInt(key[0]);
        return new Tuple2<>(newKey, newValue);
    }

    private Tuple2<Integer, List<String>> topNWords(Integer year, Iterable<String> words) {
        List<String> target = new ArrayList<>();
        words.forEach(target::add);
        List<String> subItems = new ArrayList<String>(target.subList(0, 10));
        return new Tuple2<>(year, subItems);
    }
}
