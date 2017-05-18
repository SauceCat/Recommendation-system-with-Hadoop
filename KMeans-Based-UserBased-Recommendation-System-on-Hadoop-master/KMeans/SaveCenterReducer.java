package KMeans;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.VectorWritable;

public class SaveCenterReducer extends Reducer<IntWritable, VectorWritable, IntWritable, VectorWritable>{
	public void reduce(IntWritable cluster, Iterable<VectorWritable> centers, Context context)
			throws IOException, InterruptedException{
		for(VectorWritable center : centers){
			context.write(cluster, center);
			break;
		}
	}
}
