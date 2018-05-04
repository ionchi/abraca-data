package task1_3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.AmazonFFRConstants;

import java.io.IOException;

public class Product2userMapper extends
        Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, Context context)
                throws IOException,InterruptedException {

        if (key.get() != 0) {
            String[] values = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            if (values.length != 10) {
                return;
            }
            String Product = values[AmazonFFRConstants.PRODUCT_ID];
            String User = values[AmazonFFRConstants.USER_ID];

            context.write(new Text(User), new Text(Product));
        }
    }
}