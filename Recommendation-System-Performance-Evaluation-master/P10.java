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

public class P10 {
	
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
	
	
	///////////////////////////////////////////////////////////////
	// Stage 2: Check the accuracy of first recommend for each user
	///////////////////////////////////////////////////////////////
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
	
	// check the accuracy of the first recommended item 
	public static class AccReducer extends Reducer<Text, Text, Text, Text>{
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
			
			float score = 0;
			if(actual.contains(predict.get(0))){
				score = 1;
			}
			context.write(user, new Text(Float.toString(score)));
		}
	}
	
	
	/////////////////////////
	// Stage 3: Calculate P10
	/////////////////////////
	public static class P10Mapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException{
			String[] parts = value.toString().split("\t");
			context.write(new Text("Acc"), new Text(parts[1]));	
		}
	}	
	
	public static class P10Reducer extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> Accs, Context context) 
				throws IOException, InterruptedException{
			float sum = 0;
			int count = 0;
			for(Text Acc : Accs){
				sum += Float.parseFloat(Acc.toString());
				count ++;
			}
			float P10 = sum / count;
			context.write(new Text("P10"), new Text(Float.toString(P10)));
		}
	}
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: P10 <actual> <predict>");
	      System.exit(2);
	    }
	    
	    //////////////////////////////////////
	    // Stage 1: Get the true pref of users
	    //////////////////////////////////////
	    Job Actual = new Job(conf, "P10");
	    Actual.setJarByClass(P10.class);
	    
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
	    FileOutputFormat.setOutputPath(Actual, new Path("P10/Actual"));
	    
	    Actual.waitForCompletion(true);
	    
	    
		///////////////////////////////////////////////////////////////
		// Stage 2: Check the accuracy of first recommend for each user
		///////////////////////////////////////////////////////////////
	  	Job Acc = new Job(conf, "P10");
	  	Acc.setJarByClass(P10.class);
	  	
	  	// Map Settings
	  	MultipleInputs.addInputPath(Acc, new Path(otherArgs[1]), TextInputFormat.class, PredictionMapper.class);
	  	MultipleInputs.addInputPath(Acc, new Path("P10/Actual"), TextInputFormat.class, ActualMapper.class);
	  	Acc.setMapOutputKeyClass(Text.class);
	  	Acc.setMapOutputValueClass(Text.class);
	    
	  	// Reduce Settings
	  	Acc.setReducerClass(AccReducer.class);
	  	Acc.setOutputKeyClass(Text.class);
	  	Acc.setOutputValueClass(Text.class);
	  	
	  	// Set output path
	  	FileOutputFormat.setOutputPath(Acc, new Path("P10/Acc"));
	  	Acc.waitForCompletion(true);
	  	
	  	
		/////////////////////////
		// Stage 3: Calculate P10
		/////////////////////////
		Job P10 = new Job(conf, "P10");
		P10.setJarByClass(P10.class);
		
		// Map Settings
		P10.setMapperClass(P10Mapper.class);
		P10.setMapOutputKeyClass(Text.class);
		P10.setMapOutputValueClass(Text.class);
		
		// Reduce Settings
		P10.setReducerClass(P10Reducer.class);
		P10.setOutputKeyClass(Text.class);
		P10.setOutputValueClass(Text.class);
		
		// Set output path
		FileInputFormat.addInputPath(P10, new Path("P10/Acc"));
		FileOutputFormat.setOutputPath(P10, new Path("P10/P10"));
	  	
	    System.exit(P10.waitForCompletion(true) ? 0 : 1);
	  }
}
