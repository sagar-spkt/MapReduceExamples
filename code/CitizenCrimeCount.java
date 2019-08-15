import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class CitizenCrimeCount {
    public static class MapperOutput implements Writable {

        public final static String CRIME = new String("CRIME");
        public final static String CITIZEN = new String("CITIZEN");
        private String type = new String();  // indicates tuple is from crime or citizen file
        private String citizenName = new String();
        private long crimeCount = 0;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getCitizenName() {
            return citizenName;
        }

        public void setCitizenName(String citizenName) {
            this.citizenName = citizenName;
        }

        public long getCrimeCount() {
            return crimeCount;
        }

        public void setCrimeCount(long crimeCount) {
            this.crimeCount = crimeCount;
        }

        public void readFields(DataInput in) throws IOException {
            type = in.readUTF();
            if (type.equals(CRIME)) {
                crimeCount = in.readLong();
            } else if (type.equals(CITIZEN)) {
                citizenName = in.readUTF();
            }
        }

        public void write(DataOutput out) throws IOException {
            out.writeUTF(type);
            if (type.equals(CRIME)) {
                out.writeLong(crimeCount);
            } else if (type.equals(CITIZEN)) {
                out.writeUTF(citizenName);
            }
        }

        public String toString() {
            if (type.equals(CRIME)) {
                return type + "\t" + crimeCount;
            } else if (type.equals(CITIZEN)) {
                return type + "\t" + citizenName;
            }
            return "";
        }
    }

    public static class CrimeMapper extends Mapper<Object, Text, Text, MapperOutput> {

        private final static long one = 1;
        private MapperOutput mapperOutput = new MapperOutput();
        private Text citizenId = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",", -1);  // CSV File line
            if (fields != null && fields.length == 4 && fields[0].length() > 0) {
                citizenId.set(fields[0]);
                mapperOutput.setType(MapperOutput.CRIME);
                mapperOutput.setCrimeCount(one);
                context.write(citizenId, mapperOutput);
            }
        }
    }

    public static class CitizenMapper extends Mapper<Object, Text, Text, MapperOutput> {

        private Text citizenId = new Text();
        private MapperOutput mapperOutput = new  MapperOutput();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",", -1);
            if (fields != null && fields.length == 6 && fields[0].length() > 0) {
                citizenId.set(fields[0]);
                mapperOutput.setType(MapperOutput.CITIZEN);
                mapperOutput.setCitizenName(fields[1] + " " + fields[2]);
                context.write(citizenId, mapperOutput);
            }
        }
    }

    public static class CrimeCountCombiner extends Reducer<Text, MapperOutput, Text, MapperOutput> {
        public void reduce(Text key, Iterable<MapperOutput> values, Context context) throws IOException, InterruptedException {
            long crimeCount = 0;
            for (MapperOutput val: values) {
                if (val.getType().equals(MapperOutput.CRIME)) {
                    crimeCount += val.getCrimeCount();
                } else if (val.getType().equals(MapperOutput.CITIZEN)) {
                    context.write(key, val);
                }
            }
            MapperOutput combinedCount = new MapperOutput();
            combinedCount.setType(MapperOutput.CRIME);
            combinedCount.setCrimeCount(crimeCount);
            context.write(key, combinedCount);
        }
    }
    
    public static class JoinAndReduce extends Reducer<Text, MapperOutput, Text, LongWritable> {

        private Text citizenName = new Text();

        public void reduce(Text key, Iterable<MapperOutput> values, Context context) throws IOException, InterruptedException {
            long crimeCount = 0;
            for (MapperOutput val: values) {
                if (val.getType().equals(MapperOutput.CRIME)) {
                    crimeCount += val.getCrimeCount();
                } else if (val.getType().equals(MapperOutput.CITIZEN)) {
                    citizenName.set(val.getCitizenName());
                }
            }
            context.write(citizenName, new LongWritable(crimeCount));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "citizen crime count");
        job.setJarByClass(CitizenCrimeCount.class);
        job.setCombinerClass(CrimeCountCombiner.class);
        job.setReducerClass(JoinAndReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapperOutput.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CrimeMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, CitizenMapper.class);
        TextOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}