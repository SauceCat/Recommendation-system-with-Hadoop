package KMeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.mahout.math.VectorWritable;

import KMeans.UpdateJobRunner.Result;

public class KMeans {
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: KMeans <input> <k>");
	      System.exit(2);
	    }
	    
		////////////////////////////////////////////
		// Stage 1: make user-item preference matrix
		////////////////////////////////////////////
	    
	    Job UserItemPref = new Job(new Configuration(), "kmeans_UserItemPref");
	    UserItemPref.setJarByClass(KMeans.class);
	    
	    // Map settings
	    UserItemPref.setMapperClass(UserItemPrefMapper.class);
	    UserItemPref.setMapOutputKeyClass(VLongWritable.class);
	    UserItemPref.setMapOutputValueClass(VLongWritable.class);
	    
	    // Reduce settings
	    UserItemPref.setReducerClass(UserItemPrefReducer.class);
	    UserItemPref.setOutputKeyClass(VLongWritable.class);
	    UserItemPref.setOutputValueClass(VectorWritable.class);
	    
	    // Set the input output format
	    UserItemPref.setInputFormatClass(TextInputFormat.class);
	    UserItemPref.setOutputFormatClass(SequenceFileOutputFormat.class);
	    
	    // Set path
		TextInputFormat.addInputPath(UserItemPref, new Path(otherArgs[0]));
	    SequenceFileOutputFormat.setOutputPath(UserItemPref, new Path("UserBased/KMeans/UserItemPref"));
	    UserItemPref.waitForCompletion(true);
	    
	    
		//////////////////////////////////////////////////////////
		// Stage 2: randomly choose k users as the initial centers
		//////////////////////////////////////////////////////////
	    
	    Job Init = new Job(new Configuration(), "kmeans_Init");
	    Init.getConfiguration().setInt("k", Integer.parseInt(otherArgs[1]));
	    Init.setJarByClass(KMeans.class);
	    
	    // Map settings
	    Init.setMapperClass(InitMapper.class);
	    Init.setMapOutputKeyClass(IntWritable.class);
	    Init.setMapOutputValueClass(VectorWritable.class);
	    
	    // Reduce settings
	    Init.setReducerClass(InitReducer.class);
	    Init.setOutputKeyClass(IntWritable.class);
	    Init.setOutputValueClass(VectorWritable.class);
	    
	    // Set the input output format
	    Init.setInputFormatClass(SequenceFileInputFormat.class);
	    Init.setOutputFormatClass(SequenceFileOutputFormat.class);
	    
	    // Set path
	    SequenceFileInputFormat.addInputPath(Init, new Path("UserBased/KMeans/UserItemPref"));
	    SequenceFileOutputFormat.setOutputPath(Init, new Path("UserBased/KMeans/Jobs/Jobs_0"));
	    
	    Init.waitForCompletion(true);
	    
	   
		//////////////////////////////////
		// Stage 3: do iteration to update
		//////////////////////////////////
	    int maxIterations = 5000;
	    Result result = UpdateJobRunner.runUpdateJobs(maxIterations);
	    
	    
		//////////////////////////////////////////
		// Stage 4: save the final result: cluster
		//////////////////////////////////////////
	    Job SaveCluster = new Job(new Configuration(), "kmeans_SaveCluster");
	    SaveCluster.setJarByClass(KMeans.class);
	    
	    // Map settings
	    SaveCluster.setMapperClass(SaveClusterMapper.class);
	    SaveCluster.setMapOutputKeyClass(IntWritable.class);
	    SaveCluster.setMapOutputValueClass(VectorWritable.class);
	    
	    // Reduce settings
	    SaveCluster.setReducerClass(SaveClusterReducer.class);
	    SaveCluster.setOutputKeyClass(IntWritable.class);
	    SaveCluster.setOutputValueClass(VectorWritable.class);
	    
	    // Set the input output format
	    SaveCluster.setInputFormatClass(SequenceFileInputFormat.class);
	    SaveCluster.setOutputFormatClass(SequenceFileOutputFormat.class);
	    
	    // Set path
	    SequenceFileInputFormat.addInputPath(SaveCluster, new Path("UserBased/KMeans/Jobs/Jobs_" + result.iters + "/Cluster/output-r-00000"));
	    SequenceFileOutputFormat.setOutputPath(SaveCluster, new Path("UserBased/KMeans/Result/Cluster"));
	    
	    SaveCluster.waitForCompletion(true);
	    
	    
		/////////////////////////////////////////
		// Stage 5: save the final result: center
		/////////////////////////////////////////
	    Job SaveCenter = new Job(new Configuration(), "kmeans_SaveCenter");
	    SaveCenter.setJarByClass(KMeans.class);
	    
	    // Map settings
	    SaveCenter.setMapperClass(SaveCenterMapper.class);
	    SaveCenter.setMapOutputKeyClass(IntWritable.class);
	    SaveCenter.setMapOutputValueClass(VectorWritable.class);
	    
	    // Reduce settings
	    SaveCenter.setReducerClass(SaveCenterReducer.class);
	    SaveCenter.setOutputKeyClass(IntWritable.class);
	    SaveCenter.setOutputValueClass(VectorWritable.class);
	    
	    // Set the input output format
	    SaveCenter.setInputFormatClass(SequenceFileInputFormat.class);
	    SaveCenter.setOutputFormatClass(SequenceFileOutputFormat.class);
	    
	    // Set path
	    SequenceFileInputFormat.addInputPath(SaveCenter, new Path("UserBased/KMeans/Jobs/Jobs_" + result.iters + "/Center/output-r-00000"));
	    SequenceFileOutputFormat.setOutputPath(SaveCenter, new Path("UserBased/KMeans/Result/Center"));
	    
	    SaveCenter.waitForCompletion(true);
	    
	    
		////////////////////////////
		// Stage 6: print out result
		////////////////////////////
	    System.out.println("============================");
        System.out.println("KMeans execution successful.");
        System.out.println("----------------------------");
        System.out.println("Number of Iterations: " + result.iters);
        System.out.println("Final centroids:");
        
        for(String center : result.result){
        	System.out.println(center);
        }
        System.out.println("============================");

        System.exit(0);
	 }
}
