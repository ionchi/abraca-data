package task1_3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class Product2userReduce extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> users = new LinkedList<>();
        for (Text value : values)
            users.add(value.toString());
        for(int i=0;i<users.size();i++)
            for(int j=i+1;j<users.size();j++) {
                String couple = users.get(i) + "\t" + users.get(j);
                context.write(new Text(couple),key);
            }
    }

}