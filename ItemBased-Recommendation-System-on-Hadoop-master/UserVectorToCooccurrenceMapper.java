package ItemBased;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/*
 user {song1 : 1.0, song2 : 1.0, song3 : 1.0...} -> user song1 ; user song2 ; user song3... 
 */
public class UserVectorToCooccurrenceMapper 
extends Mapper<VLongWritable,VectorWritable,VLongWritable,VLongWritable> {
	public void map(VLongWritable userID, VectorWritable userVector, Context context) 
			throws IOException, InterruptedException {
		Iterator<Vector.Element> it = ((RandomAccessSparseVector) userVector.get()).iterateNonZero();
		while(it.hasNext()){
			int index1 = it.next().index();
			Iterator<Vector.Element> it2 = ((RandomAccessSparseVector) userVector.get()).iterateNonZero();
			while(it2.hasNext()){
				int index2 = it2.next().index();
				context.write(new VLongWritable(index1), new VLongWritable(index2));
			}
		}
	}
}
