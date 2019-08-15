import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MinMaxOccuringWords {
    public static class WordCountTuple implements Writable {
        private String word = new String();
        private long count = 0;
    
        public String getWord() {
            return word;
        }
    
        public void setWord(String word) {
            this.word = word;
        }
    
        public long getCount() {
            return count;
        }
    
        public void setCount(long count) {
            this.count = count;
        }
    
        public void readFields(DataInput in) throws IOException {
            word = in.readUTF();
            count = in.readLong();
        }
    
        public void write(DataOutput out) throws IOException {
            out.writeUTF(word);
            out.writeLong(count);
        }
    
        public String toString() {
            return word + "\t" + count;
        }
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        // Same as WordCount Mapper
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
            while (stringTokenizer.hasMoreTokens()) {
                word.set(stringTokenizer.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        // Same as WordCount Reducer
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val: values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class MinMaxWordReducer extends Reducer<Text, IntWritable, Text, WordCountTuple> {

        private final static Text min = new Text("min");
        private final static Text max = new Text("max");
        // To collect all words with same min and max count using ArrayList
        private ArrayList<WordCountTuple> minTuples = new ArrayList<WordCountTuple>();
        private ArrayList<WordCountTuple> maxTuples = new ArrayList<WordCountTuple>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable val: values) {
                count += val.get();
            }

            WordCountTuple newTuple = new WordCountTuple();
            newTuple.setWord(key.toString());
            newTuple.setCount(count);

            if (minTuples.isEmpty() || maxTuples.isEmpty()) {
                // If first key add to both min and max array
                if (minTuples.isEmpty()) {
                    minTuples.add(newTuple);
                }

                if (maxTuples.isEmpty()) {
                    maxTuples.add(newTuple);
                }
            } else {
                // if found lower/upper count, clear array and add new min or max word and
                // if count just match to min/max count, append the word
                if (minTuples.get(0).getCount() == 0 || minTuples.get(0).getCount() > count) {
                    minTuples.clear();
                    minTuples.add(newTuple);
                } else if (minTuples.get(0).getCount() == count) {
                    minTuples.add(newTuple);
                }

                if (maxTuples.get(0).getCount() == 0 || maxTuples.get(0).getCount() < count) {
                    maxTuples.clear();
                    maxTuples.add(newTuple);
                } else if (maxTuples.get(0).getCount() == count) {
                    maxTuples.add(newTuple);
                }
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            // write to file only after reducer has gone through all words from map
            for (WordCountTuple maxTuple: maxTuples) {
                context.write(max, maxTuple);
            }
            for (WordCountTuple minTuple: minTuples) {
                context.write(min, minTuple);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "min max word occurance");
        job.setJarByClass(MinMaxOccuringWords.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(WordCountCombiner.class);
        job.setReducerClass(MinMaxWordReducer.class);
        // Number of Reducer task should be 1 otherwise
        // we get min/max occuring word per reducer task
        job.setNumReduceTasks(1);  
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
