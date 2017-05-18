package ItemBased;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Reducer;

/*
 song user1 user2 user3... -> song sqrt(usercount)
 */
public class ItemUserReducer extends Reducer<VLongWritable, VLongWritable, VLongWritable, Text>{
	public void reduce(VLongWritable itemID, Iterable<VLongWritable> users, Context context) 
			throws IOException, InterruptedException{
		float sum = 0;
		for(@SuppressWarnings("unused") VLongWritable user : users){
			sum += 1;
		}
		sum = (float) Math.sqrt(sum);
		context.write(itemID, new Text(Float.toString(sum)));
	}
}