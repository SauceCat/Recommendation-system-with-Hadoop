package ItemBased;

import java.io.IOException;

import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.math.VectorWritable;

// Just wrap up the cooccurrence vector 
public class CooccurrenceColumnWrapperMapper 
extends Mapper<VLongWritable,VectorWritable,VLongWritable,VectorOrPrefWritable>{
	public void map(VLongWritable key, VectorWritable value, Context context) 
			throws IOException, InterruptedException{
		context.write(key, new VectorOrPrefWritable(value.get()));
	}
}
