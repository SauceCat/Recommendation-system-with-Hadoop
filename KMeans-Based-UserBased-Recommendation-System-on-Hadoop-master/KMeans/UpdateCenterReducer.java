package KMeans;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class UpdateCenterReducer 
extends Reducer<IntWritable, VectorWritable, IntWritable, VectorWritable>{
	private MultipleOutputs<IntWritable, VectorWritable> out;
	int jobID;

	protected void setup(Context context) throws IOException,InterruptedException {
		Configuration conf = context.getConfiguration();
		jobID = Integer.parseInt(conf.get("jobID"));
		out = new MultipleOutputs<IntWritable, VectorWritable>(context);
		FileSystem fs = FileSystem.get(new Configuration());
		// true stands for recursively deleting the folder you gave
		fs.delete(new Path("UserBased/KMeans/Jobs/Jobs_" + (jobID - 1)), true);
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
	  out.close();
	}
	
	public void reduce(IntWritable cluster, Iterable<VectorWritable> userVectors, Context context)
			throws IOException, InterruptedException{
		Vector sumup = null;
		int count = 0;
		for(VectorWritable userVector : userVectors){
			sumup = sumup == null ? userVector.get() : sumup.plus(userVector.get());
			out.write(cluster, userVector, "Cluster/output");
			count++;
		}
		sumup = sumup.divide(count);
		// context.write(cluster, new VectorWritable(sumup));
		out.write(cluster, new VectorWritable(sumup), "Center/output");
	}
}
