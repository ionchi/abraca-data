package task1_3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Product2user {


    public static void main(String[] args) throws Exception {

        //adding timestamp to the path prevent failures if old tmp folder still exists.
        String tmpFolderPath =  "tmp_"+ Long.toString(System.currentTimeMillis());
        //temporary output path for 1st reducer
        Path tmpPath = new Path(tmpFolderPath);

        Job job1 = null;
        Job job2 = null;
        try {
            job1 = new Job(new Configuration(), Product2user.class.getSimpleName()+"1");
            job2 = new Job(new Configuration(), Product2user.class.getSimpleName()+"2");
        } catch (IOException e) {
            System.out.println("[ERR] Error while creating new Job");
            e.printStackTrace();
            System.exit(1);
        }

        job1.setJarByClass(Product2user.class);
        job1.setMapperClass(Product2userMapper.class);
        job1.setReducerClass(Product2userReduce.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, tmpPath);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.waitForCompletion(true);

        job2.setJarByClass(Product2user.class);
        job2.setMapperClass(Product2userMapper2.class);
        job2.setReducerClass(Product2userReduce2.class);

        FileInputFormat.addInputPath(job2, tmpPath);
        FileOutputFormat.setOutputPath(job2,new Path(args[1]) );

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.waitForCompletion(true);

    }

}
