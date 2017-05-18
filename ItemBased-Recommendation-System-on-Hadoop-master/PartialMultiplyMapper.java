package ItemBased;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.item.VectorAndPrefsWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/*
 itemID similarityMatrixColumn, userIDs, prefValues -> userID partialProduct
 */
public class PartialMultiplyMapper 
extends Mapper<VLongWritable,VectorAndPrefsWritable,VLongWritable,VectorWritable>{
	public void map(VLongWritable key, VectorAndPrefsWritable vectorAndPrefsWritable, Context context) 
			throws IOException, InterruptedException{
		Vector cooccurrenceColumn = vectorAndPrefsWritable.getVector();
		List<Long> userIDs = vectorAndPrefsWritable.getUserIDs();
		List<Float> prefValues = vectorAndPrefsWritable.getValues();
		for(int i = 0; i < userIDs.size(); i++){
			long userID = userIDs.get(i);
			float prefValue = prefValues.get(i);
			Vector partialProduct = cooccurrenceColumn.times(prefValue);
			context.write(new VLongWritable(userID), new VectorWritable(partialProduct));
		}
	}
}
