package task1_3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class Product2userReduce extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> products = new HashSet<String>();
        for (Text value : values)
            products.add(value.toString());
        for(String s:products){
            for(String r:products){
                if(s.compareTo(r)>0) {
                    String couple = s + "\t" + r;
                    context.write(new Text(couple), key);
                }
            }
        }
    }

}