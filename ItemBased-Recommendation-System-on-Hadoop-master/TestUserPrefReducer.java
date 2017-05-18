package ItemBased;

import java.io.IOException;

import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/*
user song1 song2 song3... -> user {song1 : 1.0, song2 : 1.0, song3 : 1.0...}
*/
public class TestUserPrefReducer extends Reducer<VLongWritable, VLongWritable, VLongWritable, VectorWritable>{
	public void reduce(VLongWritable userID, Iterable<VLongWritable> itemPrefs, Context context) 
			throws IOException, InterruptedException{
		Vector userVector = new RandomAccessSparseVector(Integer.MAX_VALUE, 100);
		for(VLongWritable itemPref : itemPrefs){
			userVector.set((int) itemPref.get(), 1.0f);
		}
		context.write(userID, new VectorWritable(userVector));
	}
}