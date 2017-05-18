package ItemBased;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/*
 index1 index2@sim index3@sim index4@sim... -> index1 {index2 : sim, index3 : sim, index4 : sim...}
 */
public class SimilarityReducer 
extends Reducer<VLongWritable,Text,VLongWritable,VectorWritable> {
	public void reduce(VLongWritable Index1, Iterable<Text> Index2s,Context context) 
			throws IOException, InterruptedException {
		
		List<ArrayList<Float>> SimItems = new ArrayList<ArrayList<Float>>();
		for(Text text : Index2s){
			ArrayList<Float> SimItem = new ArrayList<Float>();
			float index2 = Float.parseFloat(text.toString().split("@")[0]);
			float sim = Float.parseFloat(text.toString().split("@")[1]);
			SimItem.add(index2);
			SimItem.add(sim);
			SimItems.add(SimItem);
		}
		Collections.sort(SimItems, new Comparator<ArrayList<Float>>() {
            public int compare(ArrayList<Float> first, ArrayList<Float> second) {
            	return second.get(1).compareTo(first.get(1));
            }
        });
		
		int neighbor = (int) (SimItems.size()*1.0);
		Vector cooccurrenceRow = new RandomAccessSparseVector(Integer.MAX_VALUE, neighbor);
		for(int i = 0; i < neighbor; i++){
			int index = (int)((double)SimItems.get(i).get(0));
			float sim = SimItems.get(i).get(1);
			cooccurrenceRow.set(index, sim);
		}
		context.write(Index1, new VectorWritable(cooccurrenceRow));
		
	}
}