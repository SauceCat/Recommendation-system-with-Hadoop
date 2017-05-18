package KMeans;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VectorWritable;

public class SaveCenterMapper extends Mapper<IntWritable,VectorWritable,IntWritable,VectorWritable>{
	public void map(IntWritable cluster, VectorWritable center, Context context)
			throws IOException, InterruptedException{
		context.write(cluster, center);
	}
}
