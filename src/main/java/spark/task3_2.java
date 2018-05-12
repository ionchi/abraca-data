package spark;

import org.apache.commons.collections.map.MultiValueMap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import utils.Utils;

import java.io.Serializable;
import java.util.*;

import static utils.AmazonFFRConstants.PRODUCT_ID;
import static utils.AmazonFFRConstants.SCORE;
import static utils.AmazonFFRConstants.TIME;

public class task3_2 implements Serializable {


    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        new task3_2().run();
        long elapsed = System.currentTimeMillis() - start;
        System.out.println("TEMPO TRASCORSO = "+elapsed/1000.0+" secondi.");
    }

    private void run() {

        String logFile = "Esercizi/Reviews.csv";
        SparkConf conf = new SparkConf().setAppName(this.getClass().getSimpleName());
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> input = jsc.textFile(logFile).cache();
        input.mapToPair(row -> this.splitRow(row))
                .reduceByKey((a,b) ->  new Tuple2<Integer, Float>(a._1 + b._1, a._2 + b._2))
                .mapValues(a -> a._1 / a._2)
                .mapToPair(row -> this.changeMapping(row))
                .groupByKey()
                .mapToPair(row -> this.selectScore(row))
                .sortByKey()
                .saveAsTextFile("Esercizi/outputspark");

        jsc.stop();
        jsc.close();

    }

    @SuppressWarnings("unchecked")
    private Tuple2<String, String> selectScore(Tuple2<String, Iterable<String>> row) {
        MultiValueMap score2products = new MultiValueMap();
        for (String s : row._2) {
            StringTokenizer st = new StringTokenizer(s, "\t");
            String yearId = st.nextToken();
            if(Integer.parseInt(yearId)>2002 && Integer.parseInt(yearId)<2013){
                float score = Float.parseFloat(st.nextToken());
                score2products.put(yearId, score);
            }}
        List<String> years = new LinkedList<String>();
        years.addAll(score2products.keySet());
        years.sort(Collections.reverseOrder());
        String out = "";
        for(String year : years) {
            Iterator<Float> it = score2products.getCollection(year).iterator();
            while(it.hasNext()) {

                out += "\n";
                out += "              ";
                out += year;
                out += " ";
                out += it.next();

            }
        }
        return new Tuple2<>(row._1,out);
    }



    private Tuple2<String, String> changeMapping(Tuple2<String, Float> tuple) {
        StringTokenizer st = new StringTokenizer(tuple._1, "\t");
        String yearId = st.nextToken();
        String productID = st.nextToken();
        return new Tuple2<>(productID, yearId + "\t" + tuple._2);
    }

    private Tuple2<String, Tuple2<Integer, Float>> splitRow(String row) {
        String[] fields = row.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        if (fields.length != 10) {
            return new Tuple2<>("404" + "\t" + "Errore", new Tuple2<>(2, new Float(1)));
        }
        String productID = fields[PRODUCT_ID];
        long time = Long.parseLong(fields[TIME]);
        int score = Integer.parseInt(fields[SCORE]);
        String yearId = Utils.unix2StringYear(time);
        return new Tuple2<>(yearId + "\t" + productID , new Tuple2<>(score, new Float(1)));
    }

}