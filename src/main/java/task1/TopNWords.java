package task1;

import org.apache.commons.collections.map.MultiValueMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import utils.AmazonFFRConstants;
import utils.Utils;

import java.io.IOException;
import java.util.*;

public class TopNWords {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: TopNWords <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf);
        job.setJobName("Top N");
        job.setJarByClass(TopNWords.class);
        job.setMapperClass(TopNWordsMapper.class);
        job.setReducerClass(TopNWordsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * The mapper reads one line at the time, splits it into an array of single words and emits every
     * word to the reducers with the value of 1.
     */
    public static class TopNWordsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private String tokens = "[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']";
        Text summary = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() != 0) {
                String[] parts = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
                summary.set(parts[AmazonFFRConstants.SUMMARY]);
                String cleanLine = summary.toString().toLowerCase().replaceAll(tokens, " ");
                StringTokenizer itr = new StringTokenizer(cleanLine);
                long time = Long.parseLong(parts[AmazonFFRConstants.TIME]);
                String yearId = Utils.unix2StringYear(time);
                while (itr.hasMoreTokens()) {
                    word.set(itr.nextToken().trim());
                    String newKey = yearId + "\t" + word;
                    context.write(new Text(newKey), one);
                }
            }
        }
    }

    /**
     * The reducer retrieves every word and puts it into a Map: if the word already exists in the
     * map, increments its value, otherwise sets it to 1.
     */
    public static class TopNWordsReducer extends Reducer<Text, IntWritable, Text, Text> {

        private Map<Text, IntWritable> countMap = new HashMap<Text, IntWritable>();
        private static final int YEARID = 0;
        private static final int WORD = 1;
        private Map<String, MultiValueMap> words4Year = new LinkedHashMap<String, MultiValueMap>();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            String[] fields = key.toString().split("\t");
            String year = fields[YEARID];
            String word = fields[WORD];

            // computes the number of occurrences of a single word
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            if (!this.words4Year.containsKey(year)) {
                MultiValueMap mvMap = new MultiValueMap();
                mvMap.put(sum, word);
                this.words4Year.put(year,mvMap);
            }
            else
                this.words4Year.get(year).put(sum,word);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (String year : this.words4Year.keySet()) {
                List<Integer> scores = this.orderedScores(this.words4Year.get(year).keySet());
                String out = this.topProducts(scores, year);
                context.write(new Text(year), new Text(out));
            }
        }

        private List<Integer> orderedScores(Set<Integer> set) {
            ArrayList<Integer> scores = new ArrayList<Integer>();
            scores.addAll(set);
            scores.sort(Collections.reverseOrder());
            return scores;
        }

        @SuppressWarnings("unchecked")
        private String topProducts(List<Integer> scores, String dateID){
            String out = "";
            int counter = 0;
            int lastScore = 0;
            for (Integer score : scores) {
                Iterator<String> it = this.words4Year.get(dateID).getCollection(score).iterator();
                while(it.hasNext())	{
                    if(counter<10) {
                        out += " ";
                        out += it.next();
                        out += " ";
                        out += score;
                        counter++;
                        lastScore = score;
                    }
                    else if(counter==10 && score == lastScore) {
                        out += " ";
                        out += it.next();
                        out += " ";
                        out += score;

                    }
                    else {
                        break;
                    }
                }
            }
            return out;
        }
    }
}
