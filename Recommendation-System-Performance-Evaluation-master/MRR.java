package Evaluation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class MRR {
	
	//////////////////////////////////////
	// Stage 1: Get the true pref of users
	//////////////////////////////////////
	public static class UserPrefMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException{
			Text user = new Text();
			Text song = new Text();
			String[] parts = value.toString().split("\t");
			user.set(parts[0]);
			song.set(parts[1]);
			context.write(user, song);			
		}
	}
	
	// Reduce the pref into a list
	public static class UserPrefReducer extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text user, Iterable<Text> prefs, Context context) 
				throws IOException, InterruptedException{
			String longprefs = "";
			for(Text pref : prefs){
				longprefs = longprefs + '\t' + pref.toString();
			}
			context.write(user, new Text(longprefs));
		}
	}
	
	
	///////////////////////////////////////
	// Stage 2: Calculate RR for each user
	///////////////////////////////////////
	public static class PredictionMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException{
			String[] parts = value.toString().split("\t");
			Text user = new Text(parts[0]);
			String longpred = "predict";
			for(int i = 1; i < parts.length; i++){
				if(!parts[i].equals("")){
					longpred = longpred + "\t" + parts[i];
				}else{
					continue;
				}
			}
			context.write(user, new Text(longpred));
		}
	}
	
	public static class ActualMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException{
			String[] parts = value.toString().split("\t");
			Text user = new Text(parts[0]);
			String longpref = "actual";
			for(int i = 1; i < parts.length; i++){
				if(!parts[i].equals("")){
					longpref = longpref + "\t" + parts[i];
				}else{
					continue;
				}
			}
			context.write(user, new Text(longpref));
		}
	}
	
	// Calculate RR for each user 
	public static class RRReducer extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text user, Iterable<Text> ActualOrPred, Context context) 
				throws IOException, InterruptedException{
			List<String> predict = new ArrayList<String>();
			List<String> actual = new ArrayList<String>();
			for(Text aop : ActualOrPred){
				List<String> parts = new ArrayList<String>(Arrays.asList(aop.toString().split("\t")));
				if(parts.get(0).equals("predict")){
					predict = parts.subList(1, parts.size());
				}
				if(parts.get(0).equals("actual")){
					actual = parts.subList(1, parts.size());
				}
			}
			// MRR calculation
			float score = 0;
			for(int i = 0; i < predict.size(); i++){
				if(actual.contains(predict.get(i))){
					score += 1.0 / (i + 1.0);
					break;
				}else{
					continue;
				}
			}
			context.write(user, new Text(Float.toString(score)));
		}
	}
	
	
	/////////////////////////
	// Stage 3: Calculate MRR
	/////////////////////////
	public static class MRRMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException{
			String[] parts = value.toString().split("\t");
			context.write(new Text("RR"), new Text(parts[1]));	
		}
	}	
	
	public static class MRRReducer extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> RRs, Context context) 
				throws IOException, InterruptedException{
			float sum = 0;
			int count = 0;
			for(Text RR : RRs){
				sum += Float.parseFloat(RR.toString());
				count ++;
			}
			float MRR = sum / count;
			context.write(new Text("MRR"), new Text(Float.toString(MRR)));
		}
	}
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: MRR <actual> <predict>");
	      System.exit(2);
	    }
	    
	    //////////////////////////////////////
	    // Stage 1: Get the true pref of users
	    //////////////////////////////////////
	    Job Actual = new Job(conf, "MRR");
	    Actual.setJarByClass(MRR.class);
	    
	    // Map Settings
	    Actual.setMapperClass(UserPrefMapper.class);
	    Actual.setMapOutputKeyClass(Text.class);
	    Actual.setMapOutputValueClass(Text.class);
	    
	    // Reduce Settings
	    Actual.setReducerClass(UserPrefReducer.class);
	    Actual.setOutputKeyClass(Text.class);
	    Actual.setOutputValueClass(Text.class);
	    
	    // Set input output path
	    FileInputFormat.addInputPath(Actual, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(Actual, new Path("MRR/Actual"));
	    
	    Actual.waitForCompletion(true);
	    
	    
	    //////////////////////////////////////
	    // Stage 2: Calculate RR for each user
	    //////////////////////////////////////
	  	Job RR = new Job(conf, "MRR");
	  	RR.setJarByClass(MRR.class);
	  	
	  	// Map Settings
	  	MultipleInputs.addInputPath(RR, new Path(otherArgs[1]), TextInputFormat.class, PredictionMapper.class);
	  	MultipleInputs.addInputPath(RR, new Path("MRR/Actual"), TextInputFormat.class, ActualMapper.class);
	  	RR.setMapOutputKeyClass(Text.class);
	  	RR.setMapOutputValueClass(Text.class);
	    
	  	// Reduce Settings
	  	RR.setReducerClass(RRReducer.class);
	  	RR.setOutputKeyClass(Text.class);
	  	RR.setOutputValueClass(Text.class);
	  	
	  	// Set output path
	  	FileOutputFormat.setOutputPath(RR, new Path("MRR/RR"));
	  	RR.waitForCompletion(true);
	  	
	  	
		/////////////////////////
		// Stage 3: Calculate MRR
		/////////////////////////
		Job MRR = new Job(conf, "MRR");
		MRR.setJarByClass(MRR.class);
		
		// Map Settings
		MRR.setMapperClass(MRRMapper.class);
		MRR.setMapOutputKeyClass(Text.class);
		MRR.setMapOutputValueClass(Text.class);
		
		// Reduce Settings
		MRR.setReducerClass(MRRReducer.class);
		MRR.setOutputKeyClass(Text.class);
		MRR.setOutputValueClass(Text.class);
		
		// Set output path
		FileInputFormat.addInputPath(MRR, new Path("MRR/RR"));
		FileOutputFormat.setOutputPath(MRR, new Path("MRR/MRR"));
	  	
	    System.exit(MRR.waitForCompletion(true) ? 0 : 1);
	  }
}
