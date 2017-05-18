package UserBased;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/*
 prepare the user history preference
 */

public class FilterMapper extends Mapper<VLongWritable,VectorWritable,VLongWritable,Text>{
	public void map(VLongWritable key, VectorWritable value, Context context) 
			throws IOException, InterruptedException{
		Vector temp = value.get();
		Iterator<Vector.Element> it = ((RandomAccessSparseVector) temp).iterateNonZero();
		String out = "filter" + "";
		while(it.hasNext()){
			Vector.Element e = it.next();
			int itemIndex = e.index();
			out = out + itemIndex + "\t";
		}
		context.write(key, new Text(out));
	}
}