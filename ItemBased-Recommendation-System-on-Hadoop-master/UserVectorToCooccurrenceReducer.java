package ItemBased;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

/*
 song1 song2 song2 song3 song2 -> song1 song2@3.0; song1 song3@1.0...
 */
public class UserVectorToCooccurrenceReducer 
extends Reducer<VLongWritable,VLongWritable,VLongWritable,Text> {
	public void reduce(VLongWritable itemIndex1, Iterable<VLongWritable> itemIndex2s, Context context) 
			throws IOException, InterruptedException {
		Vector cooccurrenceRow = new RandomAccessSparseVector(Integer.MAX_VALUE, 100);
		List<Integer> index2s = new ArrayList<Integer>();
		for (VLongWritable intWritable : itemIndex2s) {
			int itemIndex2 = (int) intWritable.get();
			if(!index2s.contains(itemIndex2)){
				index2s.add(itemIndex2);
			}
			cooccurrenceRow.set(itemIndex2, cooccurrenceRow.get(itemIndex2) + 1.0);
		}
		for(Integer index2 : index2s){
			String out = Integer.toString(index2) + "@" + Integer.toString((int) cooccurrenceRow.get(index2));
			context.write(itemIndex1, new Text(out));
		}
	}
}
