package ItemBased;

import java.io.IOException;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VectorWritable;

/*
 get all the item scores
 */
public class RecommendMapper
extends Mapper<VLongWritable,VectorWritable,VLongWritable,VectorWritable>{
	public void map(VLongWritable key, VectorWritable value, Context context) 
			throws IOException, InterruptedException{
		context.write(key, value);
	}
}
