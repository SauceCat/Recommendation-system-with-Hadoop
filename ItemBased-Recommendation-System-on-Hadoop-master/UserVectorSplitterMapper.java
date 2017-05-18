package ItemBased;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/*
 user{song1 : 1.0, song2 : 1.0, song3 : 1.0} -> user {song1 : 1.0}, user {song2 : 1.0}, user {song3 : 1.0}
 */
public class UserVectorSplitterMapper 
extends Mapper<VLongWritable, VectorWritable, VLongWritable, VectorOrPrefWritable>{
	public void map(VLongWritable key, VectorWritable value, Context context) 
			throws IOException, InterruptedException{
		long userID = key.get();
		Vector userVector = value.get();
		Iterator<Vector.Element> it = ((RandomAccessSparseVector) userVector).iterateNonZero();
		while(it.hasNext()){
			Vector.Element e = it.next();
			int itemIndex = e.index();
			float preferenceValue = (float) e.get();
			context.write(new VLongWritable(itemIndex), new VectorOrPrefWritable(userID, preferenceValue));
		}
	}
}
