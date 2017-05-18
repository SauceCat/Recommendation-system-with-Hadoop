package ItemBased;

import java.io.IOException;

import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

// Aggregate the vector with the same user
public class AggregateCombiner 
extends Reducer<VLongWritable,VectorWritable,VLongWritable,VectorWritable>{
	public void reduce(VLongWritable key, Iterable<VectorWritable> values, Context context)
			throws IOException, InterruptedException{
		Vector partial = null;
		for(VectorWritable vectorWritable : values){
			partial = partial == null ? vectorWritable.get() : partial.plus(vectorWritable.get());
		}
		context.write(key, new VectorWritable(partial));
	}
}
