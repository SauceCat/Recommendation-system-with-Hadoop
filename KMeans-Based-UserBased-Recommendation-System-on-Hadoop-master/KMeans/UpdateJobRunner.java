package KMeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;


public class UpdateJobRunner {
	
	public static ArrayList<Vector> C_old = new ArrayList<Vector>();
	public static ArrayList<Vector> C_new = new ArrayList<Vector>();
	
	// The iteration jobs
	public static Job createUpdateJob(int jobID) 
			throws IOException, InterruptedException{
		Job updateJob = new Job(new Configuration(), "kmeans_job" + jobID);
		updateJob.getConfiguration().setInt("jobID", jobID);
		updateJob.setJarByClass(KMeans.class);
		
		// Map settings
		// Update the clusters
		updateJob.setMapperClass(UpdateClusterMapper.class);
		updateJob.setMapOutputKeyClass(IntWritable.class);
		updateJob.setMapOutputValueClass(VectorWritable.class);
		
		// Reduce settings
		// Update the centers
		updateJob.setReducerClass(UpdateCenterReducer.class);
		updateJob.setOutputKeyClass(IntWritable.class);
		updateJob.setOutputValueClass(VectorWritable.class);
		
		// Set the input output format
		updateJob.setInputFormatClass(SequenceFileInputFormat.class);
		updateJob.setOutputFormatClass(SequenceFileOutputFormat.class);
	    
	    // Set path
		SequenceFileInputFormat.addInputPath(updateJob, new Path("UserBased/KMeans/Jobs/Jobs_" + (jobID-1) + "/Cluster/output-r-00000"));
	    SequenceFileOutputFormat.setOutputPath(updateJob, new Path("UserBased/KMeans/Jobs/Jobs_" + jobID));
		
		return updateJob;
	}
	
	// The summary job
	public static Job SummaryResult(int jobID) 
			throws IOException, InterruptedException{
		Job summaryJob = new Job(new Configuration(), "summary");
		summaryJob.setJarByClass(KMeans.class);
		
		// Map settings
		// Update the clusters
		summaryJob.setMapperClass(SummaryMapper.class);
		summaryJob.setMapOutputKeyClass(IntWritable.class);
		summaryJob.setMapOutputValueClass(IntWritable.class);
		
		// Reduce settings
		// Update the centers
		summaryJob.setReducerClass(SummaryReducer.class);
		summaryJob.setOutputKeyClass(Text.class);
		summaryJob.setOutputValueClass(IntWritable.class);
		
		// Set the input output format
		summaryJob.setInputFormatClass(SequenceFileInputFormat.class);
		summaryJob.setOutputFormatClass(TextOutputFormat.class);
	    
	    // Set path
		SequenceFileInputFormat.addInputPath(summaryJob, new Path("UserBased/KMeans/Jobs/Jobs_" + jobID + "/Cluster/output-r-00000"));
	    SequenceFileOutputFormat.setOutputPath(summaryJob, new Path("UserBased/KMeans/Summary"));
		
		return summaryJob;
	}
	
	// Function to judge if the iteration converged
	public static boolean is_converged(ArrayList<Vector> C_old, ArrayList<Vector> C_new){
		for(int i = 0; i < C_old.size(); i++){
			if(!C_old.equals(C_new)){
				return false;
			}
		}
		return true;
	}
	
	public static class Result{
		public int iters;
		public ArrayList<String> result;
	}
	
	public static ArrayList<Vector> GetCenter(int jobID){
		Configuration conf = new Configuration();
		ArrayList<Vector> Cs = new ArrayList<Vector>();
		try{
			Path path = new Path("hdfs://localhost:9000/user/mac/UserBased/KMeans/Jobs/Jobs_" + jobID + "/Center/output-r-00000");
			FileSystem fs = FileSystem.get(conf);
			SequenceFile.Reader reader = new Reader(fs, path, conf);
		    IntWritable key = (IntWritable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		    VectorWritable value = (VectorWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
		    while (reader.next(key, value)) {
		    	Cs.add(value.get());
		    }
		    IOUtils.closeStream(reader);
		    return Cs;
	    }catch(IOException e){
			e.printStackTrace();
		}
	    return null;
	 }

	public static Result runUpdateJobs(int maxIterations) 
			throws IOException, ClassNotFoundException, InterruptedException{
		int numIterations = 0;
		boolean converged = false;
		int finalJob = maxIterations;
		
		for(int i = 1; i <= maxIterations; i++){
			numIterations++;
			ArrayList<Vector> C_old = GetCenter(i-1);
			Job job = createUpdateJob(i);
			job.waitForCompletion(true);
			ArrayList<Vector> C_new = GetCenter(i);
			converged = is_converged(C_old, C_new);
			if(converged){
				finalJob = i;
				break;
			}
		}
		
		Job SummaryJob = SummaryResult(finalJob);
		SummaryJob.waitForCompletion(true);
		
		ArrayList<String> summary = new ArrayList<String>();
		try{
			Path path = new Path("hdfs://localhost:9000/user/mac/UserBased/KMeans/Summary/part-r-00000");
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
			String word = null;
			while((word = br.readLine()) != null){
				summary.add(word);
			}
		}catch(IOException e){
			e.printStackTrace();
		}
		
		Result out = new Result();
		out.iters = numIterations;
		out.result = summary;
		return out;
	}
}
