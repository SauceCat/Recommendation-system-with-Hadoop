package ItemBased;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/*
 prepare the user history preference
 */
public class FilterMapper extends Mapper<VLongWritable,VectorWritable,VLongWritable,VectorWritable>{
	public void map(VLongWritable key, VectorWritable value, Context context) 
			throws IOException, InterruptedException{
		Vector temp = value.get();
		Iterator<Vector.Element> it = ((RandomAccessSparseVector) temp).iterateNonZero();
		Vector out = new RandomAccessSparseVector(Integer.MAX_VALUE, 100);
		while(it.hasNext()){
			Vector.Element e = it.next();
			int itemIndex = e.index();
			out.set(itemIndex, -1000.0);
		}
		context.write(key, new VectorWritable(out));
	}
}