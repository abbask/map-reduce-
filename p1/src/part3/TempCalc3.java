import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
 
public class TempCalc3 {
 

 public static class DoubleArrayWritable extends ArrayWritable { 
	public DoubleArrayWritable() { 
		super(DoubleWritable.class);
	}
 }

 private static class JobRunner implements Runnable {
        private JobControl jobControl;

        public JobRunner(JobControl jobControl) {
                this.jobControl = jobControl;
        }

        @Override
        public void run() {
                this.jobControl.run();
        }

 }

    

 public static class MapAvg extends Mapper<LongWritable, Text, Text, IntWritable> {
       
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);

	Text word =  new Text();

        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            context.write(word, new IntWritable(Integer.parseInt(tokenizer.nextToken())));
        }
    }
 } 


 public static class ReduceAvg extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        double num = 0;
        double sum = 0;
        for (IntWritable val : values) {
        	num++;
            sum += (double)val.get();
        }
        context.write(key, new DoubleWritable(sum/num));
    }
 }
 

 public static class MapMax extends Mapper<LongWritable, Text, Text, ArrayWritable> {
 
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
	String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);

	Text word = new Text();
	Text wordMinus = new Text();
	Text wordPlus = new Text();

        while (tokenizer.hasMoreTokens()) {
		
		// To find the current zipcode
		String str = tokenizer.nextToken();
		word.set(str); // The first of two is always a zipcode
		
		// To find the adjacent zipcodes
		int intWord = Integer.parseInt(str);
		int intWordPlus = (intWord+1)%1000;
		int intWordMinus = (intWord-1)%1000;
		if (intWordMinus == -1) intWordMinus = 999;

                wordPlus.set(String.format("%03d", intWordPlus));
                wordMinus.set(String.format("%03d", intWordMinus));
		
		// To find the temperature
		DoubleWritable temp = new DoubleWritable(Double.parseDouble(tokenizer.nextToken()));

		// The transient array to hold previous, current, and next flag and the temperature
		DoubleWritable innercarrier[] = new DoubleWritable[2];
		DoubleArrayWritable carrier = new DoubleArrayWritable();

		// Output three pairs, one for the current, one for the last, and one for the next zipcode
		innercarrier[0] = new DoubleWritable(0);
		innercarrier[1] = temp;
		carrier.set(innercarrier);
		context.write(word, carrier);
		
                innercarrier[0] = new DoubleWritable(-1);
                innercarrier[1] = temp;
                carrier.set(innercarrier);
                context.write(wordMinus, carrier);

                innercarrier[0] = new DoubleWritable(1);
                innercarrier[1] = temp;
                carrier.set(innercarrier);
                context.write(wordPlus, carrier);
        }
    }
 } 
        
 public static class ReduceMax extends Reducer<Text, ArrayWritable, Text, DoubleWritable> {

    public void reduce(Text key, Iterable<ArrayWritable> values, Context context) 
      throws IOException, InterruptedException {
        double avg = 0;
        double avgMinus = 0;
        double avgPlus = 0;

        for (ArrayWritable val : values) {
  		Writable[] data = val.get();
		DoubleWritable left = (DoubleWritable)(data[0]);
		DoubleWritable right = (DoubleWritable)(data[1]);
		if ((double)left.get() == 0){ avg = (double)right.get(); }
		else if ((double)left.get() == 1) {avgPlus = (double)right.get();}
		else if ((double)left.get() == -1) {avgMinus = (double)right.get();}
		}      	
	
	if ( avg>avgPlus && avg>avgMinus)
        	context.write(key, new DoubleWritable(avg));
    }
 }
        
 public static void main(String[] args) throws Exception {

	String inputPath = args[0];
	String tempPath = args[1];
	String outputPath = args[1];
	if (args.length == 3) outputPath = args[2];

	Configuration conf = new Configuration();

	// jobAvg definition
	Job jobAvg = new Job(conf, "avgcalc");
    
	jobAvg.setOutputKeyClass(Text.class);
	jobAvg.setOutputValueClass(IntWritable.class);
        
	jobAvg.setMapperClass(MapAvg.class);
	jobAvg.setReducerClass(ReduceAvg.class);
       
	jobAvg.setInputFormatClass(TextInputFormat.class);
	jobAvg.setOutputFormatClass(TextOutputFormat.class);
    
	jobAvg.setJarByClass(TempCalc3.class);
        
	FileInputFormat.addInputPath(jobAvg, new Path(inputPath));
	FileOutputFormat.setOutputPath(jobAvg, new Path(tempPath));

	
	// conJobAvg definition
	ControlledJob conJobAvg = new ControlledJob(conf);
	conJobAvg.setJob(jobAvg);

	// jobMax definition
        Job jobMax = new Job(conf, "maxcalc");

        jobMax.setOutputKeyClass(Text.class);
        jobMax.setOutputValueClass(DoubleArrayWritable.class);

        jobMax.setMapperClass(MapMax.class);
        jobMax.setReducerClass(ReduceMax.class);

        jobMax.setInputFormatClass(TextInputFormat.class);
        jobMax.setOutputFormatClass(TextOutputFormat.class);

        jobMax.setJarByClass(TempCalc3.class);

        FileInputFormat.addInputPath(jobMax, new Path(tempPath));
        FileOutputFormat.setOutputPath(jobMax, new Path(outputPath));
        
	// conJobMax definition
	ControlledJob conJobMax = new ControlledJob(conf);
        conJobMax.setJob(jobMax);
	conJobMax.addDependingJob(conJobAvg);

	// jobCtrl definition
	JobControl jobCtrl = new JobControl("jobctrl");
	jobCtrl.addJob(conJobAvg);
	jobCtrl.addJob(conJobMax);
	

	// thread definition
	JobRunner jobRunner = new JobRunner(jobCtrl);
	Thread jobThread = new Thread(jobRunner);
	
	// thread start, wait for completion, and stop
	jobThread.start();
	while (!jobCtrl.allFinished()) {Thread.sleep(500);}
	jobCtrl.stop();
	
	//jobAvg.waitForCompletion(true);
	
 }
        
}
