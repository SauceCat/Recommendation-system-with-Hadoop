package KMeans;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VectorWritable;

public class SummaryMapper 
extends Mapper<IntWritable,VectorWritable,IntWritable,IntWritable>{
	public void map(IntWritable cluster, VectorWritable userVector, Context context)
			throws IOException, InterruptedException{
		context.write(cluster, new IntWritable(1));
	}
}
