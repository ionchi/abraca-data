package task1_1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.map.MultiValueMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ScoreAvgReducer extends Reducer<Text, IntWritable, Text, Text> {

    private static final int YEARID = 0;
    private static final int PRODUCT_ID = 1;

    private Map<String, MultiValueMap> score2products = new LinkedHashMap<String, MultiValueMap>();


    public void reduce(Text key, Iterable<IntWritable> values, Context context) {

        float averageScore = this.averageScore(values);

        String[] fields = key.toString().split("\t");
        String dateID = fields[YEARID];
        String productID = fields[PRODUCT_ID];
        String a = fields[YEARID];
        if(Integer.parseInt(a)>2002 && Integer.parseInt(a)<2013){

            if (!this.score2products.containsKey(productID)) {
                MultiValueMap mvMap = new MultiValueMap();
                mvMap.put(averageScore, dateID);
                this.score2products.put(productID,mvMap);
            }
            else
                this.score2products.get(productID).put(averageScore,dateID);
        }
    }



    @SuppressWarnings("unchecked")
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for (String productID : this.score2products.keySet()) {
            List<Float> scores = this.orderedScores(this.score2products.get(productID).keySet());
            String out = this.topProducts(scores, productID);
            //new date format yyyy-MM
            context.write(new Text(productID), new Text(out));
        }
    }

    private float averageScore(Iterable<IntWritable> values) {
        int tot = 0;
        float sum = 0;
        for(IntWritable v : values) {
            sum += v.get();
            tot++;
        }
        return sum/tot;
    }

    private List<Float> orderedScores(Set<Float> set) {
        ArrayList<Float> scores = new ArrayList<Float>();
        scores.addAll(set);
        scores.sort(Collections.reverseOrder());
        return scores;
    }

    @SuppressWarnings("unchecked")
    private String topProducts(List<Float> scores, String dateID){
        int topProdCounter = 0;
        String out = "";
        for (Float score : scores) {
            Iterator<String> it = this.score2products.get(dateID).getCollection(score).iterator();
            while(it.hasNext())	{
                out += " ";
                out += it.next();
                out += " ";
                out += score;
                topProdCounter++;
            }
        }
        return out;
    }

}
