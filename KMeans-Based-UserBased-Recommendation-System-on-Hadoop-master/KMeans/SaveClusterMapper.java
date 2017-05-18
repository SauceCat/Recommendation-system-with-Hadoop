package KMeans;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VectorWritable;

public class SaveClusterMapper 
extends Mapper<IntWritable,VectorWritable,IntWritable,VectorWritable>{
	public void map(IntWritable cluster, VectorWritable userVector, Context context)
			throws IOException, InterruptedException{
		context.write(cluster, userVector);
	}
}
