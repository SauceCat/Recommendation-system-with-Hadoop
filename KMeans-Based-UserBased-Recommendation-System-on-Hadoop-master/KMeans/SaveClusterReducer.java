package KMeans;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.mahout.math.VectorWritable;

public class SaveClusterReducer extends Reducer<IntWritable, VectorWritable, IntWritable, VectorWritable>{
	private MultipleOutputs<IntWritable, VectorWritable> out;
	 
	protected void setup(Context context) throws IOException,
     	InterruptedException {
		out = new MultipleOutputs<IntWritable, VectorWritable>(context);
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
	  out.close();
	}
	
	public void reduce(IntWritable cluster, Iterable<VectorWritable> vectors, Context context)
			throws IOException, InterruptedException{
		for(VectorWritable vector : vectors){
			out.write(cluster, vector, "Cluster_" + cluster);
		}
	}
}
