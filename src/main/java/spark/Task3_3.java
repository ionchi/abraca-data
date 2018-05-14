package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import utils.AmazonFFRConstants;

import java.io.Serializable;

public class Task3_3 {
    public static class AmazonTable implements Serializable{

        private String ID;
        private String productId;
        private String userId;
        private String PROFILE_NAME;
        private String HELPFULNESS_NUMERATOR;
        private String HELPFULNESS_DENOMINATOR;
        private Float SCORE;
        private String TIME;
        private String SUMMARY;
        private String TEXT;


        public String getID() {
            return ID;
        }

        public void setID(String ID) {
            this.ID = ID;
        }

        public String getProductId() {
            return productId;
        }

        public void setProductId(String productId) {
            this.productId = productId;
        }

        public String getUserId() {
            return userId;
        }

        public void setUserId(String userId) {
            this.userId = userId;
        }

        public String getPROFILE_NAME() {
            return PROFILE_NAME;
        }

        public void setPROFILE_NAME(String PROFILE_NAME) {
            this.PROFILE_NAME = PROFILE_NAME;
        }

        public String getHELPFULNESS_NUMERATOR() {
            return HELPFULNESS_NUMERATOR;
        }

        public void setHELPFULNESS_NUMERATOR(String HELPFULNESS_NUMERATOR) {
            this.HELPFULNESS_NUMERATOR = HELPFULNESS_NUMERATOR;
        }

        public String getHELPFULNESS_DENOMINATOR() {
            return HELPFULNESS_DENOMINATOR;
        }

        public void setHELPFULNESS_DENOMINATOR(String HELPFULNESS_DENOMINATOR) {
            this.HELPFULNESS_DENOMINATOR = HELPFULNESS_DENOMINATOR;
        }

        public Float getSCORE() {
            return SCORE;
        }

        public void setSCORE(Float SCORE) {
            this.SCORE = SCORE;
        }

        public String getTIME() {
            return TIME;
        }

        public void setTIME(String TIME) {
            this.TIME = TIME;
        }

        public String getSUMMARY() {
            return SUMMARY;
        }

        public void setSUMMARY(String SUMMARY) {
            this.SUMMARY = SUMMARY;
        }

        public String getTEXT() {
            return TEXT;
        }

        public void setTEXT(String TEXT) {
            this.TEXT = TEXT;
        }


    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: spar-submit ... <input> <output>");
            System.exit(1);
        }
        long start = System.currentTimeMillis();
        String inputPath = args[0];
        String outputPath = args[1];
        SparkConf sparkConf = new SparkConf().setAppName("JavaSparkSQL");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(ctx);
        JavaRDD<AmazonTable> people = ctx.textFile(inputPath).map(
                new Function<String, AmazonTable>() {
                    @Override
                    public AmazonTable call(String line) {
                        String[] parts = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

                        AmazonTable person = new AmazonTable();
                        person.setProductId(parts[AmazonFFRConstants.PRODUCT_ID]);
                        person.setUserId(parts[AmazonFFRConstants.USER_ID]);
                        return person;
                    }
                });
        Dataset<Row> peopleDF = sqlContext.createDataFrame(people, AmazonTable.class);
        peopleDF.createOrReplaceTempView("people");
        Dataset<Row> pairsDF = sqlContext.sql("SELECT\n" +
                "t1.productId AS item1,\n" +
                "t2.productId AS item2,\n" +
                "COUNT(1) AS cnt\n" +
                "FROM\n" +
                "(\n" +
                "SELECT DISTINCT userId, productId\n" +
                "FROM people\n" +
                ") t1\n" +
                "JOIN\n" +
                "(\n" +
                "SELECT DISTINCT userId, productId\n" +
                "FROM people\n" +
                ") t2\n" +
                "ON (t1.userId = t2.userId)\n" +
                "GROUP BY t1.productId, t2.productId\n" +
                "HAVING t1.productId > t2.productId\n" +
                "ORDER BY t1.productId ASC");
        pairsDF.toJavaRDD().saveAsTextFile(outputPath);
        long elapsed = System.currentTimeMillis() - start;
        System.out.println("TEMPO TRASCORSO = " + elapsed / 1000.0 + " secondi.");
    }
o}
