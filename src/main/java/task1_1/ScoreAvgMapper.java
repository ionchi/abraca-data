package task1_1;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.AmazonFFRConstants;
import utils.Utils;

public class ScoreAvgMapper extends
        Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable key, Text value, Context context)
                throws IOException,InterruptedException {

        if (key.get() != 0) {
            String[] values = value.toString().split((","));
            if (values.length != 10) {
                return;
            }
            String Product = values[AmazonFFRConstants.PRODUCT_ID];

            long time = Long.parseLong(values[AmazonFFRConstants.TIME]);
            int score = Integer.parseInt(values[AmazonFFRConstants.SCORE]);

            String yearId = Utils.unix2StringYear(time);
            String newKey = yearId + "\t" + Product;

            context.write(new Text(newKey), new IntWritable(score));
        }
    }
}