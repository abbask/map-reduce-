import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class TempCalc2 {
        
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    //private final static IntWritable one = new IntWritable(0);
    private Text word = new Text();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            context.write(word, new IntWritable(Integer.parseInt(tokenizer.nextToken())));
        }
    }
 } 
        
 public static class Reduce extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        double num = 0;
        double sum = 0;
	double Ssum = 0;
        for (IntWritable val : values) {
        	num++;
        	sum += (double)val.get();
		Ssum += ((double)val.get()) * ((double)val.get());
        }
	double std = java.lang.Math.sqrt(( (num * Ssum) - (sum * sum) )/(num * num) );
        context.write(key, new DoubleWritable(std));
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "avgcalc");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    
    job.setJarByClass(TempCalc2.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}
