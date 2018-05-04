package task1_3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Product2userMapper2 extends
        Mapper<LongWritable, Text, Text, IntWritable> {
    private static final int PRODUCT1 = 0;
    private static final int PRODUCT2 = 1;
    private static final IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context)
            throws IOException,InterruptedException {
            String[] fields = value.toString().split("\t");
            String Product1 = fields[PRODUCT1];
            String Product2 = fields[PRODUCT2];
            String newKey = Product1 + "\t" + Product2;

            context.write(new Text(newKey),one);

    }
}