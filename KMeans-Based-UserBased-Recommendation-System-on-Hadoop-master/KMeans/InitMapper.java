package KMeans;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VectorWritable;

public class InitMapper extends Mapper<VLongWritable, VectorWritable, IntWritable, VectorWritable> {
	public final static IntWritable one = new IntWritable(1);
	public void map(VLongWritable userID, VectorWritable userVector, Context context)
            throws IOException, InterruptedException{
		context.write(one, userVector);
	}
}
