package KMeans;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SummaryReducer 
extends Reducer<IntWritable, IntWritable, Text, IntWritable>{

	public void reduce(IntWritable cluster, Iterable<IntWritable> counts, Context context)
			throws IOException, InterruptedException{
		int sumup = 0;
		for(@SuppressWarnings("unused") IntWritable count : counts){
			sumup++;
		}
		String outcluster = "Cluster@" + Integer.toString(cluster.get());
		context.write(new Text(outcluster), new IntWritable(sumup));
	}
}
