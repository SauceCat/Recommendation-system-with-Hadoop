package ItemBased;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.cf.taste.hadoop.item.VectorAndPrefsWritable;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.math.Vector;

/*
 output: itemID similarityMatrixColumn, userIDs, prefValues
 */
public class ToVectorAndPrefReducer 
extends Reducer<VLongWritable,VectorOrPrefWritable,VLongWritable,VectorAndPrefsWritable>{
	private final VectorAndPrefsWritable vectorAndPrefs = new VectorAndPrefsWritable();
	public void reduce(VLongWritable key, Iterable<VectorOrPrefWritable> values, Context context) 
			throws IOException, InterruptedException{
		List<Long> userIDs = new ArrayList<>();
		List<Float> prefValues = new ArrayList<>();
		Vector similarityMatrixColumn = null;
		for(VectorOrPrefWritable val : values){
			if(val.getVector() == null){
				// then this is a user-pref value
				userIDs.add(val.getUserID());
				prefValues.add(val.getValue());
			}else{
				// then this is the column vector
				if(similarityMatrixColumn != null){
					throw new IllegalStateException("Found two similarity-matrix columns for item index " + key.get());
				}
				similarityMatrixColumn = val.getVector();
			}
		}
		
		if(similarityMatrixColumn == null){
			return;
		}
		
		vectorAndPrefs.set(similarityMatrixColumn, userIDs, prefValues);
		context.write(key, vectorAndPrefs);
	}
}
