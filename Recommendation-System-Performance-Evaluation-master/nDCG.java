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

public class nDCG {
	
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
	
	
	////////////////////////////////////////
	// Stage 2: Calculate nDCG for each user
	////////////////////////////////////////
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
	
	// Calculate nDCG for each user 
	public static class nDCGReducer extends Reducer<Text, Text, Text, Text>{
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
			// nDCG calculation
			float nDCG = 0;
			for(int i = 0; i < predict.size(); i++){
				if(actual.contains(predict.get(i))){
					if(i == 0){
						nDCG += 1.0;
					}else{
						nDCG += 1.0 / Math.log(i + 1.0);
					}
				}
			}
			
			float iDCG = 1;
			for(int i = 1; i < actual.size(); i++){
				iDCG += 1.0 / Math.log(i + 1.0);
			}
			
			float score = nDCG / iDCG;
			context.write(user, new Text(Float.toString(score)));
		}
	}
	
	
	///////////////////////////////
	// Stage 3: Calculate mean nDCG
	///////////////////////////////
	public static class DCGMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException{
			String[] parts = value.toString().split("\t");
			context.write(new Text("DCG"), new Text(parts[1]));	
		}
	}	
	
	public static class DCGReducer extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> DCGs, Context context) 
				throws IOException, InterruptedException{
			float sum = 0;
			int count = 0;
			for(Text DCG : DCGs){
				sum += Float.parseFloat(DCG.toString());
				count ++;
			}
			float mnDCG = sum / count;
			context.write(new Text("nDCG"), new Text(Float.toString(mnDCG)));
		}
	}
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: nDCG <actual> <predict>");
	      System.exit(2);
	    }
	    
	    //////////////////////////////////////
	    // Stage 1: Get the true pref of users
	    //////////////////////////////////////
	    Job Actual = new Job(conf, "nDCG");
	    Actual.setJarByClass(nDCG.class);
	    
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
	    FileOutputFormat.setOutputPath(Actual, new Path("nDCG/Actual"));
	    
	    Actual.waitForCompletion(true);
	    
	    
	    ///////////////////////////////////////
	    // Stage 2: Calculate apk for each user
	    ///////////////////////////////////////
	  	Job nDCG = new Job(conf, "nDCG");
	  	nDCG.setJarByClass(nDCG.class);
	  	
	  	// Map Settings
	  	MultipleInputs.addInputPath(nDCG, new Path(otherArgs[1]), TextInputFormat.class, PredictionMapper.class);
	  	MultipleInputs.addInputPath(nDCG, new Path("nDCG/Actual"), TextInputFormat.class, ActualMapper.class);
	  	nDCG.setMapOutputKeyClass(Text.class);
	  	nDCG.setMapOutputValueClass(Text.class);
	    
	  	// Reduce Settings
	  	nDCG.setReducerClass(nDCGReducer.class);
	  	nDCG.setOutputKeyClass(Text.class);
	  	nDCG.setOutputValueClass(Text.class);
	  	
	  	// Set output path
	  	FileOutputFormat.setOutputPath(nDCG, new Path("nDCG/nDCG"));
	  	nDCG.waitForCompletion(true);
	  	
	  	
		///////////////////////////
		// Stage 3: Calculate mnDCG
		///////////////////////////
		Job mnDCG = new Job(conf, "nDCG");
		mnDCG.setJarByClass(nDCG.class);
		
		// Map Settings
		mnDCG.setMapperClass(DCGMapper.class);
		mnDCG.setMapOutputKeyClass(Text.class);
		mnDCG.setMapOutputValueClass(Text.class);
		
		// Reduce Settings
		mnDCG.setReducerClass(DCGReducer.class);
		mnDCG.setOutputKeyClass(Text.class);
		mnDCG.setOutputValueClass(Text.class);
		
		// Set output path
		FileInputFormat.addInputPath(mnDCG, new Path("nDCG/nDCG"));
		FileOutputFormat.setOutputPath(mnDCG, new Path("nDCG/mnDCG"));
	  	
	    System.exit(mnDCG.waitForCompletion(true) ? 0 : 1);
	  }
}
