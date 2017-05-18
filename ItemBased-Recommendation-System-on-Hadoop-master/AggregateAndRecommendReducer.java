package ItemBased;

import java.io.IOException;

import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/*
 The same with combiner, output: userID itemVector
 */
public class AggregateAndRecommendReducer
extends Reducer<VLongWritable, VectorWritable, VLongWritable, VectorWritable>{
	public void reduce(VLongWritable key, Iterable<VectorWritable> values, Context context)
			throws IOException, InterruptedException{
		Vector recommendationVector = null;
		for(VectorWritable vectorWritable : values){
			recommendationVector = recommendationVector == null ? 
					vectorWritable.get() : recommendationVector.plus(vectorWritable.get());
		}
		context.write(key, new VectorWritable(recommendationVector));
	}
}
