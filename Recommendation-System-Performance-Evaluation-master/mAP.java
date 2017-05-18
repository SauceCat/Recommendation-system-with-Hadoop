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

public class mAP {
	
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
	// Stage 2: Calculate apk for each user
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
	
	// Calculate apk for each user 
	public static class ApkReducer extends Reducer<Text, Text, Text, Text>{
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
			// apk calculation
			float score = 0;
			float num_hits = 0;
			for(int i = 0; i < predict.size(); i++){
				if(actual.contains(predict.get(i))){
					num_hits ++;
					score += num_hits / (i + 1.0);
				}
			}
			score = score / Math.min(10000, actual.size());
			context.write(user, new Text(Float.toString(score)));
		}
	}
	
	
	/////////////////////////
	// Stage 3: Calculate mAP
	/////////////////////////
	public static class ApkMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException{
			String[] parts = value.toString().split("\t");
			context.write(new Text("apk"), new Text(parts[1]));	
		}
	}	
	
	public static class mAPReducer extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> apks, Context context) 
				throws IOException, InterruptedException{
			float sum = 0;
			int count = 0;
			for(Text apk : apks){
				sum += Float.parseFloat(apk.toString());
				count ++;
			}
			float mAP = sum / count;
			context.write(new Text("mAP"), new Text(Float.toString(mAP)));
		}
	}
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: mAP <actual> <predict>");
	      System.exit(2);
	    }
	    
	    //////////////////////////////////////
	    // Stage 1: Get the true pref of users
	    //////////////////////////////////////
	    Job Actual = new Job(conf, "mAP");
	    Actual.setJarByClass(mAP.class);
	    
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
	    FileOutputFormat.setOutputPath(Actual, new Path("mAP/Actual"));
	    
	    Actual.waitForCompletion(true);
	    
	    
	    ///////////////////////////////////////
	    // Stage 2: Calculate apk for each user
	    ///////////////////////////////////////
	  	Job Apk = new Job(conf, "mAP");
	  	Apk.setJarByClass(mAP.class);
	  	
	  	// Map Settings
	  	MultipleInputs.addInputPath(Apk, new Path(otherArgs[1]), TextInputFormat.class, PredictionMapper.class);
	  	MultipleInputs.addInputPath(Apk, new Path("mAP/Actual"), TextInputFormat.class, ActualMapper.class);
	  	Apk.setMapOutputKeyClass(Text.class);
	  	Apk.setMapOutputValueClass(Text.class);
	    
	  	// Reduce Settings
	  	Apk.setReducerClass(ApkReducer.class);
	  	Apk.setOutputKeyClass(Text.class);
	  	Apk.setOutputValueClass(Text.class);
	  	
	  	// Set output path
	  	FileOutputFormat.setOutputPath(Apk, new Path("mAP/Apk"));
	  	Apk.waitForCompletion(true);
	  	
	  	
		/////////////////////////
		// Stage 3: Calculate mAP
		/////////////////////////
		Job mAP = new Job(conf, "mAP");
		mAP.setJarByClass(mAP.class);
		
		// Map Settings
		mAP.setMapperClass(ApkMapper.class);
		mAP.setMapOutputKeyClass(Text.class);
		mAP.setMapOutputValueClass(Text.class);
		
		// Reduce Settings
		mAP.setReducerClass(mAPReducer.class);
		mAP.setOutputKeyClass(Text.class);
		mAP.setOutputValueClass(Text.class);
		
		// Set output path
		FileInputFormat.addInputPath(mAP, new Path("mAP/Apk"));
		FileOutputFormat.setOutputPath(mAP, new Path("mAP/mAP"));
	  	
	    System.exit(mAP.waitForCompletion(true) ? 0 : 1);
	  }
}
