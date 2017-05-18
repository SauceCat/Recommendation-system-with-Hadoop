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

public class Rt {
	
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
	
	
	//////////////////////////////////////
	// Stage 2: Calculate Rt for each user
	//////////////////////////////////////
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
	
	// Calculate Rt for each user 
	public static class RtReducer extends Reducer<Text, Text, Text, Text>{
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
			// Rt calculation
			float score = 0;
			float num_hits = 0;
			for(int i = 0; i < predict.size(); i++){
				if(actual.contains(predict.get(i))){
					num_hits ++;
				}
			}
			score = num_hits / actual.size();
			context.write(user, new Text(Float.toString(score)));
		}
	}
	
	
	/////////////////////////
	// Stage 3: Calculate mRt
	/////////////////////////
	public static class mRtMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException{
			String[] parts = value.toString().split("\t");
			context.write(new Text("Rt"), new Text(parts[1]));	
		}
	}	
	
	public static class mRtReducer extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> Rts, Context context) 
				throws IOException, InterruptedException{
			float sum = 0;
			int count = 0;
			for(Text Rt : Rts){
				sum += Float.parseFloat(Rt.toString());
				count ++;
			}
			float mRt = sum / count;
			context.write(new Text("Rt"), new Text(Float.toString(mRt)));
		}
	}
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: Rt <actual> <predict>");
	      System.exit(2);
	    }
	    
	    //////////////////////////////////////
	    // Stage 1: Get the true pref of users
	    //////////////////////////////////////
	    Job Actual = new Job(conf, "Rt");
	    Actual.setJarByClass(Rt.class);
	    
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
	    FileOutputFormat.setOutputPath(Actual, new Path("Rt/Actual"));
	    
	    Actual.waitForCompletion(true);
	    
	    
	    ///////////////////////////////////////
	    // Stage 2: Calculate apk for each user
	    ///////////////////////////////////////
	  	Job Rt = new Job(conf, "Rt");
	  	Rt.setJarByClass(Rt.class);
	  	
	  	// Map Settings
	  	MultipleInputs.addInputPath(Rt, new Path(otherArgs[1]), TextInputFormat.class, PredictionMapper.class);
	  	MultipleInputs.addInputPath(Rt, new Path("Rt/Actual"), TextInputFormat.class, ActualMapper.class);
	  	Rt.setMapOutputKeyClass(Text.class);
	  	Rt.setMapOutputValueClass(Text.class);
	    
	  	// Reduce Settings
	  	Rt.setReducerClass(RtReducer.class);
	  	Rt.setOutputKeyClass(Text.class);
	  	Rt.setOutputValueClass(Text.class);
	  	
	  	// Set output path
	  	FileOutputFormat.setOutputPath(Rt, new Path("Rt/Rt"));
	  	Rt.waitForCompletion(true);
	  	
	  	
		/////////////////////////
		// Stage 3: Calculate mRt
		/////////////////////////
		Job mRt = new Job(conf, "Rt");
		mRt.setJarByClass(Rt.class);
		
		// Map Settings
		mRt.setMapperClass(mRtMapper.class);
		mRt.setMapOutputKeyClass(Text.class);
		mRt.setMapOutputValueClass(Text.class);
		
		// Reduce Settings
		mRt.setReducerClass(mRtReducer.class);
		mRt.setOutputKeyClass(Text.class);
		mRt.setOutputValueClass(Text.class);
		
		// Set output path
		FileInputFormat.addInputPath(mRt, new Path("Rt/Rt"));
		FileOutputFormat.setOutputPath(mRt, new Path("Rt/mRt"));
	  	
	    System.exit(mRt.waitForCompletion(true) ? 0 : 1);
	  }
}
