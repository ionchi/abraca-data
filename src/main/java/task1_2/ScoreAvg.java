package task1_2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ScoreAvg {


    public static void main(String[] args) throws Exception {

        Job job = null;
        try {
            job = new Job(new Configuration(), ScoreAvg.class.getSimpleName());
        } catch (IOException e) {
            System.out.println("[ERR] Error while creating new Job");
            e.printStackTrace();
            System.exit(1);
        }

        job.setJarByClass(ScoreAvg.class);
        job.setMapperClass(ScoreAvgMapper.class);
        job.setReducerClass(ScoreAvgReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }

}
